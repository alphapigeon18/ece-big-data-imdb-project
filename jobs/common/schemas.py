"""Column definitions of the seven IMDb non-commercial TSV files.

Raw files are read as strings (so that data quality can be profiled without
losing information), then cast with ``TYPES`` when building the clean layer.
"""

IMDB_FILES = {
    "name_basics": "name.basics.tsv.gz",
    "title_akas": "title.akas.tsv.gz",
    "title_basics": "title.basics.tsv.gz",
    "title_crew": "title.crew.tsv.gz",
    "title_episode": "title.episode.tsv.gz",
    "title_principals": "title.principals.tsv.gz",
    "title_ratings": "title.ratings.tsv.gz",
}

COLUMNS = {
    "name_basics": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title_akas": [
        "titleId", "ordering", "title", "region", "language", "types",
        "attributes", "isOriginalTitle",
    ],
    "title_basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title_crew": ["tconst", "directors", "writers"],
    "title_episode": [
        "tconst", "parentTconst", "seasonNumber", "episodeNumber",
    ],
    "title_principals": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title_ratings": ["tconst", "averageRating", "numVotes"],
}

# Target type for every non-string column. "array" = comma separated list.
TYPES = {
    "name_basics": {
        "birthYear": "int", "deathYear": "int",
        "primaryProfession": "array", "knownForTitles": "array",
    },
    "title_akas": {
        "ordering": "int", "types": "array", "attributes": "array",
        "isOriginalTitle": "int",
    },
    "title_basics": {
        "isAdult": "int", "startYear": "int", "endYear": "int",
        "runtimeMinutes": "int", "genres": "array",
    },
    "title_crew": {"directors": "array", "writers": "array"},
    "title_episode": {"seasonNumber": "int", "episodeNumber": "int"},
    "title_principals": {"ordering": "int"},
    "title_ratings": {"averageRating": "double", "numVotes": "int"},
}

PRIMARY_KEYS = {
    "name_basics": ["nconst"],
    "title_akas": ["titleId", "ordering"],
    "title_basics": ["tconst"],
    "title_crew": ["tconst"],
    "title_episode": ["tconst"],
    "title_principals": ["tconst", "ordering"],
    "title_ratings": ["tconst"],
}
