"""Generate a small synthetic dataset with the exact IMDb TSV layout.

It is only used to smoke-test the pipeline in a few seconds before running
it on the real (multi-GB) files. Known data quality problems (death before
birth, orphan foreign keys, impossible years...) are injected on purpose.

Usage:
    python tests/make_sample_data.py --out data/sample
    RAW_DIR=data/sample python jobs/preparation/profile_nulls.py
"""

import argparse
import gzip
import random
from pathlib import Path

N = "\\N"
GENRES = {
    "Drama": 110, "Comedy": 95, "Action": 115, "Thriller": 105,
    "Horror": 90, "Romance": 100, "Documentary": 85, "Sci-Fi": 112,
    "Crime": 108, "Animation": 88,
}
COUNTRIES = [
    ("US", "en", 0.45), ("GB", "en", 0.10), ("FR", "fr", 0.12),
    ("DE", "de", 0.08), ("JP", "ja", 0.08), ("ES", "es", 0.07),
    ("IT", "it", 0.06), ("IN", "hi", 0.04),
]
WORDS = [
    "night", "river", "love", "shadow", "city", "dream", "storm", "fire",
    "silence", "road", "winter", "summer", "ghost", "heart", "king", "star",
    "blood", "garden", "mirror", "ocean", "last", "first", "lost", "secret",
]


def title_words(rng, k=None):
    k = k or rng.randint(1, 3)
    return " ".join(rng.choice(WORDS) for _ in range(k)).title()


def write(out, name, header, rows):
    with gzip.open(out / name, "wt", encoding="utf-8", newline="") as fh:
        fh.write("\t".join(header) + "\n")
        for row in rows:
            fh.write("\t".join(str(v) for v in row) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--movies", type=int, default=4000)
    parser.add_argument("--people", type=int, default=1500)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- names
    people = []
    for i in range(1, args.people + 1):
        nconst = f"nm{i:07d}"
        birth = rng.randint(1925, 2000) if rng.random() > 0.15 else N
        death = N
        if birth != N and rng.random() < 0.08:
            death = rng.randint(min(birth + 30, 2025), 2025)
        if i % 150 == 0 and birth != N:
            death = birth - rng.randint(1, 20)   # injected: death < birth
        if i % 400 == 0:
            birth = 2035                         # injected: future birth
        name = f"{rng.choice(WORDS).title()} {rng.choice(WORDS).title()}son"
        people.append([nconst, name, birth, death, N, N])

    actors = [p[0] for p in people[: int(args.people * 0.6)]]
    crew_pool = [p[0] for p in people[int(args.people * 0.6):]]
    actor_weights = [1.0 / (r + 1) ** 0.6 for r in range(len(actors))]
    gender = {a: rng.choice(["actor", "actress"]) for a in actors}

    titles, ratings, principals, crew, akas, episodes = [], [], [], [], [], []
    professions = {p[0]: set() for p in people}
    known_for = {p[0]: [] for p in people}

    # --------------------------------------------------------------- movies
    for i in range(1, args.movies + 1):
        tconst = f"tt{i:07d}"
        ttype = rng.choices(
            ["movie", "short", "tvMovie", "video"], [0.85, 0.05, 0.05, 0.05]
        )[0]
        genres = rng.sample(list(GENRES), rng.choice([1, 1, 2, 2, 3]))
        mean_rt = GENRES[genres[0]] if ttype != "short" else 15
        runtime = max(1, int(rng.gauss(mean_rt, 12)))
        if i % 97 == 0:
            runtime = rng.choice([240, 260, 45, 50])  # outliers
        if i % 250 == 0:
            runtime = 0                               # injected: zero runtime
        if rng.random() < 0.05:
            runtime = N
        year = rng.randint(1975, 2025)
        if i % 500 == 0:
            year = 2031                               # injected: future year
        if rng.random() < 0.02:
            year = N
        country, lang, _ = rng.choices(
            COUNTRIES, [c[2] for c in COUNTRIES]
        )[0]
        original = title_words(rng)
        if lang != "en":
            original = original + " " + lang.upper()
        primary = original if lang == "en" else title_words(rng)
        titles.append([
            tconst, ttype, primary, original, 0, year, N, runtime,
            ",".join(genres) if rng.random() > 0.02 else N,
        ])

        # akas: original row + a row for the origin country + translations
        akas.append([tconst, 1, original, N, N, "original", N, 1])
        akas.append([tconst, 2, original, country, N, N, N, 0])
        order = 3
        for c2, l2, _ in rng.sample(COUNTRIES, 2):
            if c2 == country:
                continue
            akas.append([
                tconst, order, title_words(rng), c2,
                l2 if rng.random() < 0.3 else N, "imdbDisplay", N, 0,
            ])
            order += 1

        quality = rng.gauss(6.3, 1.1)
        if rng.random() < 0.9:
            ratings.append([
                tconst, round(min(10, max(1, quality)), 1),
                int(rng.lognormvariate(7, 1.6)) + 5,
            ])

        # principals: director, writer, 3-5 cast members, composer
        director = rng.choice(crew_pool)
        writer = rng.choice(crew_pool) if rng.random() > 0.3 else director
        cast = []
        while len(cast) < rng.randint(3, 5):
            a = rng.choices(actors, actor_weights)[0]
            if a not in cast:
                cast.append(a)
        order = 1
        for a in cast:
            principals.append([tconst, order, a, gender[a], N, '["Role"]'])
            professions[a].add(gender[a])
            order += 1
        if rng.random() < 0.12:                    # director also acts
            principals.append([tconst, order, director, "actor", N, N])
            professions[director].add("actor")
            order += 1
        principals.append([tconst, order, director, "director", N, N])
        order += 1
        principals.append([tconst, order, writer, "writer", "screenplay", N])
        order += 1
        principals.append([tconst, order, rng.choice(crew_pool),
                           "composer", N, N])
        professions[director].add("director")
        professions[writer].add("writer")
        crew.append([tconst, director, writer])
        for p in cast[:1] + [director]:
            if len(known_for[p]) < 4:
                known_for[p].append(tconst)

    # ------------------------------------------------------------ tv series
    ep_id = 9_000_000
    for s in range(1, 61):
        series = f"tt{8_000_000 + s:07d}"
        n_seasons = rng.randint(2, 6)
        start = rng.randint(1990, 2015)
        titles.append([
            series, "tvSeries", title_words(rng), title_words(rng), 0,
            start, start + n_seasons, 45, "Drama",
        ])
        declining = s % 3 == 0
        for season in range(1, n_seasons + 1):
            for ep in range(1, rng.randint(8, 14) + 1):
                ep_id += 1
                et = f"tt{ep_id:07d}"
                titles.append([
                    et, "tvEpisode", f"Episode {ep}", f"Episode {ep}", 0,
                    start + season - 1, N, 45, "Drama",
                ])
                episodes.append([et, series, season, ep])
                base = 8.5 - 0.5 * season if declining else 7.5
                ratings.append([
                    et, round(min(10, max(1, rng.gauss(base, 0.4))), 1),
                    rng.randint(50, 5000),
                ])

    # ------------------------------------------- injected orphan references
    for k in range(25):
        ratings.append([f"tt9{k:08d}", 7.0, 10])
        principals.append([f"tt9{k:08d}", 1, actors[k], "actor", N, N])
        principals.append([titles[k][0], 50 + k, f"nm9{k:08d}", "actor",
                           N, N])
        episodes.append([f"tt7{k:08d}", f"tt6{k:08d}", 1, 1])
    crew.append([titles[0][0] + "x", "nm9999999", N])

    for p in people:
        p[4] = ",".join(sorted(professions[p[0]])) or N
        p[5] = ",".join(known_for[p[0]]) or N

    write(out, "name.basics.tsv.gz",
          ["nconst", "primaryName", "birthYear", "deathYear",
           "primaryProfession", "knownForTitles"], people)
    write(out, "title.basics.tsv.gz",
          ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
           "startYear", "endYear", "runtimeMinutes", "genres"], titles)
    write(out, "title.akas.tsv.gz",
          ["titleId", "ordering", "title", "region", "language", "types",
           "attributes", "isOriginalTitle"], akas)
    write(out, "title.crew.tsv.gz", ["tconst", "directors", "writers"], crew)
    write(out, "title.episode.tsv.gz",
          ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
          episodes)
    write(out, "title.principals.tsv.gz",
          ["tconst", "ordering", "nconst", "category", "job", "characters"],
          principals)
    write(out, "title.ratings.tsv.gz",
          ["tconst", "averageRating", "numVotes"], ratings)
    print(f"sample dataset written to {out.resolve()}")


if __name__ == "__main__":
    main()
