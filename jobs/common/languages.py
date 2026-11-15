"""Original language of a film, inferred from title.akas.

IMDb does not publish the original language of a title. It is inferred
from the alternative titles (akas) that carry the *original* title:

1. keep the akas whose ``title`` equals ``title.basics.originalTitle``;
2. language of such a row = its ``language`` column when filled, otherwise
   the main language of its ``region`` (``REGION_LANGUAGE`` below);
3. the original language is the most frequent language among those rows
   (ties: lowest ``ordering``, i.e. the earliest entry).
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.io_utils import read_table

# Main language of the IMDb region codes (ISO 3166 + IMDb historical codes).
# World-wide / multi-country codes (XWW, XEU, XAS...) are deliberately absent.
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "CA": "en", "AU": "en", "NZ": "en", "IE": "en",
    "ZA": "en", "FR": "fr", "BE": "fr", "LU": "fr", "SN": "fr", "CI": "fr",
    "DE": "de", "AT": "de", "XWG": "de", "DDDE": "de", "CH": "de",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "VE": "es", "UY": "es", "CU": "es", "IT": "it", "PT": "pt", "BR": "pt",
    "NL": "nl", "SE": "sv", "NO": "no", "DK": "da", "FI": "fi", "IS": "is",
    "PL": "pl", "CZ": "cs", "CSHH": "cs", "SK": "sk", "HU": "hu",
    "RO": "ro", "BG": "bg", "GR": "el", "TR": "tr", "RU": "ru", "SUHH": "ru",
    "UA": "uk", "BY": "be", "RS": "sr", "XYU": "sr", "YUCS": "sr",
    "HR": "hr", "SI": "sl", "BA": "bs", "MK": "mk", "AL": "sq",
    "EE": "et", "LV": "lv", "LT": "lt", "GE": "ka", "AM": "hy",
    "IL": "he", "EG": "ar", "MA": "ar", "DZ": "ar", "TN": "ar", "LB": "ar",
    "SA": "ar", "AE": "ar", "IQ": "ar", "SY": "ar", "JO": "ar", "IR": "fa",
    "AF": "fa", "PK": "ur", "IN": "hi", "BD": "bn", "LK": "si", "NP": "ne",
    "CN": "zh", "TW": "zh", "HK": "zh", "SG": "en", "JP": "ja", "KR": "ko",
    "KP": "ko", "TH": "th", "VN": "vi", "ID": "id", "MY": "ms", "PH": "tl",
    "KZ": "kk", "UZ": "uz", "MN": "mn", "NG": "en", "KE": "sw", "ET": "am",
}


def region_language_df(spark):
    return spark.createDataFrame(
        list(REGION_LANGUAGE.items()), "region string, region_language string"
    )


def original_languages(spark):
    """DataFrame (tconst, original_language, n_evidence)."""
    basics = read_table(spark, "title_basics").select(
        "tconst", "originalTitle"
    )
    akas = read_table(spark, "title_akas").select(
        F.col("titleId").alias("tconst"), "ordering", "title", "region",
        "language",
    )
    candidates = (
        akas.join(basics, "tconst")
        .where(F.col("title") == F.col("originalTitle"))
        .join(F.broadcast(region_language_df(spark)), "region", "left")
        .withColumn("lang", F.coalesce("language", "region_language"))
        .where(F.col("lang").isNotNull())
    )
    votes = candidates.groupBy("tconst", "lang").agg(
        F.count("*").alias("n_evidence"), F.min("ordering").alias("first")
    )
    best = Window.partitionBy("tconst").orderBy(
        F.desc("n_evidence"), F.asc("first")
    )
    return (
        votes.withColumn("_r", F.row_number().over(best))
        .where(F.col("_r") == 1)
        .select("tconst", F.col("lang").alias("original_language"),
                "n_evidence")
    )
