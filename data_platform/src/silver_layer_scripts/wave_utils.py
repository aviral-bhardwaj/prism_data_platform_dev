from pyspark.sql import functions as F


def derive_wave_fields(df, desc_wave_col):
    """
    Adds val_year, period, and period_alt based on desc_wave patterns:
      - 25q4 / 25Q4  -> 2025, Q4 2025, 2025 Q4
      - 25s1 / 25h1  -> 2025, H1 2025, 2025 H1
      - 2025         -> 2025, 2025, 2025

    Handles:
      - uppercase (Q/S/H)
      - leading/trailing spaces
      - non-alphanumeric separators (e.g., '25-Q4', '25 Q4')
      - non-string types (casts to string)
    """

    # normalize: cast -> trim -> lower -> remove non [0-9a-z]
    dw = F.regexp_replace(
        F.lower(F.trim(F.col(desc_wave_col).cast("string"))),
        r"[^0-9a-z]",
        ""
    )

    yy = F.regexp_extract(dw, r"^(\d{2})[qsh]\d$", 1)
    kind = F.regexp_extract(dw, r"^\d{2}([qsh])\d$", 1)
    num = F.regexp_extract(dw, r"^\d{2}[qsh](\d)$", 1)

    val_year = (
        F.when(dw.rlike(r"^\d{2}[qsh]\d$"), F.lit(2000) + yy.cast("int"))
         .when(dw.rlike(r"^\d{4}$"), dw.cast("int"))
         .otherwise(F.lit(None).cast("int"))
    )

    period = (
        F.when(kind == "q", F.concat(F.lit("Q"), num, F.lit(" "), val_year.cast("string")))
         .when((kind == "s") | (kind == "h"), F.concat(F.lit("H"), num, F.lit(" "), val_year.cast("string")))
         .when(dw.rlike(r"^\d{4}$"), val_year.cast("string"))
         .otherwise(F.lit(None))
    )

    period_alt = (
        F.when(kind == "q", F.concat(val_year.cast("string"), F.lit(" Q"), num))
         .when((kind == "s") | (kind == "h"), F.concat(val_year.cast("string"), F.lit(" H"), num))
         .when(dw.rlike(r"^\d{4}$"), val_year.cast("string"))
         .otherwise(F.lit(None))
    )

    return (
        df
        .withColumn("val_year", val_year)
        .withColumn("period", period)
        .withColumn("period_alt", period_alt)
    )
