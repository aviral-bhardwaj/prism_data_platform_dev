# Databricks notebook source
import logging
import re

from pyspark.sql import functions as F

logger = logging.getLogger("WaveLogger")


def get_active_gid_wave(silver_schema, nam_instrument, waves_to_refresh):
    gid_instrument_list = list(
        map(
            lambda row: row["gid_instrument"],
            spark.sql(  # noqa: F821 - spark injected by Databricks runtime
                f"SELECT gid_instrument FROM {silver_schema}.dim_instrument"
                f' WHERE nam_instrument = "{nam_instrument}"'
            ).collect(),
        )
    )

    active_gid_wave = ",".join(
        [
            str(row.gid_wave)
            for row in spark.table(f"{silver_schema}.dim_wave")  # noqa: F821
            .filter(F.col("active").cast("boolean") & F.col("gid_instrument").isin(gid_instrument_list))
            .select("gid_wave")
            .collect()
        ]
    )

    widget_value = waves_to_refresh

    if widget_value != "":
        cleaned_value = widget_value.replace(" ", "")
        pattern = r"^[1-9]\d*(\s*,\s*[1-9]\d*)*$"
        if not re.fullmatch(pattern, widget_value.strip()):
            msg = "Invalid input for 'waves_to_refresh'. Please enter a comma-separated list of natural numbers (e.g., 1,2,3)."
            print(msg)
            logger.error(msg, input=widget_value)
            raise ValueError(msg)

        active_gid_wave = cleaned_value
        print("Historical wave refresh\n")
        logger.info("Historical wave refresh", mode="historical")
        print("input waves: ", active_gid_wave)
        logger.info(f"input waves: {active_gid_wave}", input_waves=active_gid_wave)

        df_valid_wave = spark.sql(  # noqa: F821
            f"SELECT * FROM {silver_schema}.dim_wave"
            f" WHERE gid_instrument IN ({', '.join(map(str, gid_instrument_list))})"
            f" AND gid_wave IN ({active_gid_wave})"
        )

        valid_waves = [row.gid_wave for row in df_valid_wave.select("gid_wave").collect()]
        invalid_waves = [int(x) for x in active_gid_wave.split(",") if int(x) not in valid_waves]

        if len(invalid_waves) > 0:
            print("invalid waves for this instrument: ", ",".join(map(str, invalid_waves)))
            logger.warning("\nfound invalid waves for this instrument\n")

        if len(valid_waves) > 0:
            print("Running the notebook for the following waves: ", ",".join(map(str, valid_waves)))
            logger.info("Running the notebook for the following waves", valid_waves=valid_waves)
        else:
            logger.error("The specified wave is not available for this instrument", input=widget_value)
            dbutils.notebook.exit("The specified wave is not available for this instrument")  # noqa: F821

        return spark.sql(  # noqa: F821
            f"SELECT * FROM {silver_schema}.dim_wave"
            f" INNER JOIN {silver_schema}.dim_instrument USING (gid_instrument)"
            f" WHERE gid_wave IN ({','.join(map(str, valid_waves))})"
        )
    else:
        print("Current wave refresh\n")
        logger.info("Current wave refresh", mode="current")
        logger.info(f"active waves: {active_gid_wave}", active_waves=active_gid_wave)

        if len(active_gid_wave) != 0:
            return spark.sql(  # noqa: F821
                f"SELECT * FROM {silver_schema}.dim_wave"
                f" INNER JOIN {silver_schema}.dim_instrument USING (gid_instrument)"
                f" WHERE active = true AND nam_instrument = '{nam_instrument}'"
            )
        else:
            logger.error("No active gid_wave found", instrument=nam_instrument)
            dbutils.notebook.exit("No active gid_wave found")  # noqa: F821
