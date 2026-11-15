"""Enrichment - step 2 (plain Python, outside Spark): fetch human-written
film synopses and biographies from Wikipedia.

Pipeline (single source: Wikipedia, linked through Wikidata):

1. IMDb id -> Wikidata item -> English Wikipedia article, resolved in
   batches with the Wikidata SPARQL endpoint (property P345 = IMDb ID).
2. Films: the "Plot" section (or Synopsis / Premise / Plot summary) of the
   article plus its lead section, via the MediaWiki TextExtracts API.
3. People: the lead section of the article (biography), 20 pages / call.

Politeness / policies:
* descriptive User-Agent with a contact address (Wikimedia UA policy),
* requests are strictly sequential with a minimum interval between them,
* ``maxlag`` is sent to the MediaWiki API, HTTP 429/503 and maxlag errors
  are retried with exponential back-off and ``Retry-After`` is honoured,
* results are appended to JSONL files and already fetched ids are skipped,
  so an interrupted run resumes without re-querying anything.

Every record keeps its lineage: ``source_url`` (article), ``api_url``
(exact request) and ``wikidata_uri``. Records are written even when nothing
was found (``status`` field) so that Spark can add explicit notes.

Usage (inside the Spark container):
    CONTACT_EMAIL=you@example.com python jobs/enrichment/fetch_wikipedia.py
    ... --limit-films 50 --limit-people 150      # quick test
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402

SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://en.wikipedia.org/w/api.php"
ARTICLE_URL = "https://en.wikipedia.org/wiki/"
PLOT_HEADINGS = ["plot", "synopsis", "plot summary", "premise", "summary",
                 "story"]
SPARQL_BATCH = 200
EXTRACT_BATCH = 20   # TextExtracts limit for intro extracts

RAW_DIR = config.ENRICH_DIR / "raw"
LINKS_FILE = RAW_DIR / "wikidata_links.jsonl"
FILMS_FILE = RAW_DIR / "film_extracts.jsonl"
PEOPLE_FILE = RAW_DIR / "people_extracts.jsonl"


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class PoliteClient:
    """Sequential HTTP client with rate limiting and retry/back-off."""

    def __init__(self, contact, min_interval, max_retries=6):
        self.headers = {
            "User-Agent": (
                "ECE-BigData-IMDb-Enrichment/1.0 "
                f"(student project; contact: {contact})"
            ),
            "Accept": "application/json",
        }
        self.min_interval = min_interval
        self.max_retries = max_retries
        self._last = 0.0
        self.n_requests = 0

    def _wait(self):
        delay = self._last + self.min_interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._last = time.monotonic()

    def get_json(self, url, params, method="GET"):
        query = urllib.parse.urlencode(params)
        full_url = f"{url}?{query}"
        backoff = 2.0
        for attempt in range(self.max_retries):
            self._wait()
            if method == "POST":
                request = urllib.request.Request(
                    url, data=query.encode(), headers={
                        **self.headers,
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
            else:
                request = urllib.request.Request(
                    full_url, headers=self.headers
                )
            try:
                self.n_requests += 1
                with urllib.request.urlopen(request, timeout=60) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as err:
                if err.code in (429, 500, 502, 503, 504):
                    wait = float(err.headers.get("Retry-After") or backoff)
                    print(f"  HTTP {err.code}, retry in {wait:.0f}s")
                    time.sleep(wait)
                    backoff *= 2
                    continue
                raise
            except (urllib.error.URLError, TimeoutError) as err:
                print(f"  network error {err}, retry in {backoff:.0f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            error = {}
            if isinstance(payload, dict):
                error = payload.get("error", {})
            if error.get("code") == "maxlag":
                wait = float(error.get("lag", backoff))
                print(f"  server lagged, retry in {wait:.0f}s")
                time.sleep(max(wait, backoff))
                backoff *= 2
                continue
            return payload, full_url
        raise RuntimeError(f"giving up after {self.max_retries} tries: {url}")


def read_ids(path, column):
    with open(path, encoding="utf-8") as fh:
        return [row[column] for row in csv.DictReader(fh)]


def done_ids(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as fh:
        return {json.loads(line)["imdb_id"] for line in fh if line.strip()}


def append(path, records):
    with open(path, "a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ------------------------------------------------------------ 1. Wikidata
def resolve_links(client, imdb_ids):
    todo = [i for i in imdb_ids if i not in done_ids(LINKS_FILE)]
    print(f"[wikidata] {len(todo)} ids to resolve")
    for batch in chunks(todo, SPARQL_BATCH):
        values = " ".join(f'"{i}"' for i in batch)
        query = (
            "SELECT ?imdb ?item ?article WHERE { "
            f"VALUES ?imdb {{ {values} }} ?item wdt:P345 ?imdb . "
            "OPTIONAL { ?article schema:about ?item ; "
            "schema:isPartOf <https://en.wikipedia.org/> . } }"
        )
        payload, url = client.get_json(
            SPARQL_URL, {"query": query, "format": "json"}, method="POST"
        )
        found = {}
        for b in payload["results"]["bindings"]:
            imdb = b["imdb"]["value"]
            article = b.get("article", {}).get("value")
            # keep the item that has an English article if several match
            better = article and not found.get(imdb, {}).get("source_url")
            if imdb not in found or better:
                found[imdb] = {
                    "wikidata_uri": b["item"]["value"],
                    "source_url": article,
                }
        records = []
        for imdb in batch:
            hit = found.get(imdb)
            article = hit["source_url"] if hit else None
            title = (
                urllib.parse.unquote(article[len(ARTICLE_URL):])
                .replace("_", " ") if article else None
            )
            records.append({
                "imdb_id": imdb,
                "status": ("ok" if article else
                           "no_enwiki_article" if hit else
                           "no_wikidata_item"),
                "wikidata_uri": hit["wikidata_uri"] if hit else None,
                "wiki_title": title,
                "source_url": article,
                "api_url": SPARQL_URL,
                "retrieved_at": now(),
            })
        append(LINKS_FILE, records)
        print(f"  resolved {len(batch)} ids "
              f"({sum(r['status'] == 'ok' for r in records)} with article)")


def load_links():
    links = {}
    with open(LINKS_FILE, encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            links[record["imdb_id"]] = record
    return links


# ------------------------------------------------------- 2. film synopses
SECTION_RE = re.compile(r"^(==+)\s*(.*?)\s*\1\s*$", re.MULTILINE)


def split_sections(text):
    """Return (lead, {heading_lower: body}) from a wiki-formatted extract."""
    matches = list(SECTION_RE.finditer(text))
    lead = text[: matches[0].start()] if matches else text
    sections = {}
    for i, m in enumerate(matches):
        if len(m.group(1)) != 2:      # only level-2 headings
            continue
        end = len(text)
        for nxt in matches[i + 1:]:
            if len(nxt.group(1)) == 2:
                end = nxt.start()
                break
        body = SECTION_RE.sub("", text[m.end():end])
        sections.setdefault(m.group(2).strip().lower(), body.strip())
    return lead.strip(), sections


def fetch_films(client, film_ids, links):
    todo = [i for i in film_ids if i not in done_ids(FILMS_FILE)]
    print(f"[films] {len(todo)} synopses to fetch")
    for n, imdb in enumerate(todo, 1):
        link = links.get(imdb, {})
        base = {
            "imdb_id": imdb, "wikidata_uri": link.get("wikidata_uri"),
            "wiki_title": link.get("wiki_title"),
            "source_url": link.get("source_url"), "retrieved_at": now(),
        }
        if link.get("status") != "ok":
            append(FILMS_FILE, [{**base, "status": link.get(
                "status", "no_wikidata_item"), "api_url": None,
                "plot_heading": None, "plot_text": None, "lead_text": None}])
            continue
        payload, url = client.get_json(API_URL, {
            "action": "query", "format": "json", "formatversion": 2,
            "prop": "extracts|info", "inprop": "url", "explaintext": 1,
            "exsectionformat": "wiki", "redirects": 1, "maxlag": 5,
            "titles": link["wiki_title"],
        })
        page = payload.get("query", {}).get("pages", [{}])[0]
        if page.get("missing") or not page.get("extract"):
            append(FILMS_FILE, [{**base, "status": "page_missing",
                                 "api_url": url, "plot_heading": None,
                                 "plot_text": None, "lead_text": None}])
            continue
        lead, sections = split_sections(page["extract"])
        heading = next((h for h in PLOT_HEADINGS if sections.get(h)), None)
        append(FILMS_FILE, [{
            **base,
            "status": "ok" if heading else "no_plot_section",
            "source_url": page.get("fullurl", base["source_url"]),
            "page_id": page.get("pageid"),
            "revision_id": page.get("lastrevid"),
            "api_url": url, "plot_heading": heading,
            "plot_text": sections.get(heading) if heading else None,
            "lead_text": lead or None,
        }])
        if n % 100 == 0:
            print(f"  {n}/{len(todo)} films")


# ---------------------------------------------------------- 3. biographies
def fetch_people(client, people_ids, links):
    todo = [i for i in people_ids if i not in done_ids(PEOPLE_FILE)]
    print(f"[people] {len(todo)} biographies to fetch")
    with_page = [i for i in todo if links.get(i, {}).get("status") == "ok"]
    without = [i for i in todo if i not in set(with_page)]
    append(PEOPLE_FILE, [{
        "imdb_id": i, "status": links.get(i, {}).get(
            "status", "no_wikidata_item"),
        "wikidata_uri": links.get(i, {}).get("wikidata_uri"),
        "wiki_title": None, "source_url": None, "api_url": None,
        "bio_text": None, "retrieved_at": now(),
    } for i in without])

    for n, batch in enumerate(chunks(with_page, EXTRACT_BATCH), 1):
        by_title = {links[i]["wiki_title"]: i for i in batch}
        payload, url = client.get_json(API_URL, {
            "action": "query", "format": "json", "formatversion": 2,
            "prop": "extracts|info", "inprop": "url", "exintro": 1,
            "explaintext": 1, "exlimit": EXTRACT_BATCH, "redirects": 1,
            "maxlag": 5, "titles": "|".join(by_title),
        })
        query = payload.get("query", {})
        # follow title normalisation and redirects back to the IMDb id
        alias = {}
        for step in query.get("normalized", []) + query.get("redirects", []):
            alias[step["to"]] = alias.get(step["from"], step["from"])
        pages = {p.get("title"): p for p in query.get("pages", [])}
        records = []
        for title, imdb in by_title.items():
            page = next(
                (p for t, p in pages.items()
                 if t == title or alias.get(t) == title
                 or alias.get(alias.get(t, ""), "") == title),
                None,
            )
            ok = page is not None and page.get("extract")
            records.append({
                "imdb_id": imdb, "status": "ok" if ok else "page_missing",
                "wikidata_uri": links[imdb]["wikidata_uri"],
                "wiki_title": title,
                "source_url": (page or {}).get(
                    "fullurl", links[imdb]["source_url"]),
                "api_url": url,
                "bio_text": page.get("extract") if ok else None,
                "retrieved_at": now(),
            })
        append(PEOPLE_FILE, records)
        if n % 20 == 0:
            print(f"  {n * EXTRACT_BATCH}/{len(with_page)} people")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-films", type=int, default=None)
    parser.add_argument("--limit-people", type=int, default=None)
    parser.add_argument("--min-interval", type=float, default=float(
        os.environ.get("MIN_INTERVAL", 0.5)),
        help="seconds between two requests (default 0.5)")
    args = parser.parse_args()

    contact = os.environ.get("CONTACT_EMAIL")
    if not contact:
        sys.exit("Set CONTACT_EMAIL (required by the Wikimedia UA policy).")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    films = read_ids(config.ENRICH_DIR / "films_to_fetch.csv", "tconst")
    people = read_ids(config.ENRICH_DIR / "people_to_fetch.csv", "nconst")
    films = films[: args.limit_films]
    people = people[: args.limit_people]

    client = PoliteClient(contact, args.min_interval)
    start = time.monotonic()
    resolve_links(client, films + people)
    links = load_links()
    fetch_films(client, films, links)
    fetch_people(client, people, links)
    print(f"done: {client.n_requests} requests in "
          f"{time.monotonic() - start:.0f}s")


if __name__ == "__main__":
    main()
