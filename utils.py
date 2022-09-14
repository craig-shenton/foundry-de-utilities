# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           utils.py
DESCRIPTION:    generic ETL utility functions
CONTRIBUTORS:   Craig Shenton
CONTACT:        craig.shenton@nhs.net
CREATED:        25 March 2022
VERSION:        0.0.1
"""

# Imports
# -------------------------------------------------------------------------

# Pyspark:
from pyspark.sql.types import StringType  # used to change column type to string, alt: DateType, FloatType
from itertools import chain
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from typing import Dict

# Functions
# -------------------------------------------------------------------------


def replace_col_strings_from_dic(df: DataFrame, map_dict: Dict, column_name: str) -> DataFrame:
    """
    Method for replacing column values from one value to another using a dictionary
    Args:
        df (DataFrame): Dataframe to operate on
        map_dict (Dict): Dictionary containing the values to map from and to
        column_name (str): The column containing the values to be mapped
    Returns:
        DataFrame
    """
    assert column_name in df.schema.names, "Expected column not present in input df"
    # check if col dtype is not a string
    if df.select(column_name).dtypes != "string":
        # then cast col as string dtype
        df = df.withColumn(column_name, df[column_name].cast(StringType()))
    # replace subset col with values from a dictonary, or null if missing
    df = df.replace(to_replace=map_dict, subset=column_name)
    return df


def map_column_values(df: DataFrame, map_dict: Dict, column_name: str, new_column_name: str) -> DataFrame:
    """Method for mapping column values from one value to another using a dictionary
    Args:
        df (DataFrame): Dataframe to operate on
        map_dict (Dict): Dictionary containing the values to map from and to
        column_name (str): The column containing the values to be mapped
        new_column_name (str, optional): The name of the column to store the mapped values in.
                                    If not specified the values will be stored in the original column
    Returns:
        DataFrame
    """
    assert column_name in df.schema.names, "Expected column not present in input df"
    spark_map = F.create_map([F.lit(x) for x in chain(*map_dict.items())])
    return df.withColumn(new_column_name or column_name, spark_map[df[column_name]])


def datestring_to_timestamp_parse(df: DataFrame, date_column: str, date_format: str) -> DataFrame:
    """
    Parses the date column given the date format in a spark dataframe
    Args:
        df: Spark dataframe having a date column
        date_column: Name of the date column
        date_format: Simple Date Format (e.g., "dd/MM/yyyy") of the dates in the date column
    Returns:
        DataFrame with a parsed timestamped column
    """
    df = df.withColumn(date_column, F.to_timestamp(F.col(date_column), date_format))
    # Spark returns 'null' if the parsing fails, so first check the count of null values
    # If parse_fail_count = 0, return parsed column else raise error
    parse_fail_count = df.select(
        ([F.count(F.when(F.col(date_column).isNull(), date_column))])
    ).collect()[0][0]
    if parse_fail_count == 0:
        return df
    else:
        raise ValueError(
            f"Incorrect date format '{date_format}' for date column '{date_column}'"
        )


def melt_wide_to_long(df, id_vars, value_vars, var_name, value_name):
    """Convert :class:`DataFrame` from wide to long format."""
    _vars_and_vals = F.array(*[F.struct(F.lit(c).alias(var_name),
                                        F.col(c).alias(value_name))
                               for c in value_vars])

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def dedupe_df(df, col_filter, on_col: str) -> DataFrame:
    # Remove Duplicates
    w = W.partitionBy(col_filter).orderBy(F.desc(on_col))
    df_dedupe = df.withColumn("rank", F.dense_rank().over(w)).filter("rank = 1").drop("rank")
    return df_dedupe
