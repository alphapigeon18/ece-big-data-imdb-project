"""Title normalisation shared by the streaming lookup and the stream job.

Wikipedia page titles and IMDb titles are compared on a normalised key:
lower case, accents removed, trailing disambiguator "(...)" removed,
punctuation removed and whitespace collapsed.

    "Inception (2010 film)" -> "inception"
    "Amélie"                -> "amelie"
    "Spider-Man: No Way Home" -> "spider man no way home"
"""

from pyspark.sql import functions as F

DISAMBIGUATOR = r"\s*\(([^()]*)\)\s*$"


def normalize_key(column):
    c = column if not isinstance(column, str) else F.col(column)
    c = F.regexp_replace(c, DISAMBIGUATOR, "")
    c = F.lower(c)
    c = F.translate(
        c,
        "àáâãäåçèéêëìíîïñòóôõöùúûüýÿœæ",
        "aaaaaaceeeeiiiinooooouuuuyyoa",
    )
    c = F.regexp_replace(c, r"[^\p{L}\p{N}]+", " ")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def disambiguator(column):
    """Text inside the trailing parentheses, lower case ('' when none)."""
    c = column if not isinstance(column, str) else F.col(column)
    return F.lower(F.regexp_extract(c, DISAMBIGUATOR, 1))
