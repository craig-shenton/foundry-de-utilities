# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           test_map_column_values.py
DESCRIPTION:    unit-test of 'map_column_values' utility function
CONTRIBUTORS:   Craig Shenton
CONTACT:        craig.shenton@nhs.net
CREATED:        25 March 2022
VERSION:        0.0.1
"""

# Imports
# -------------------------------------------------------------------------

# Python:
import pytest

# Pyspark:
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Local:
from project import utils

# Globals
# -------------------------------------------------------------------------

# column names for input dataset
col_name = ["ID", "Age_ID"]


# Test: Check function output if input column is Int type
# -------------------------------------------------------------------------

def test_map_int_col_from_dic(spark_session):
    # data schema of input data
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Age_ID", IntegerType(), True)
        ]
    )
    dictionary = {"1": "a"}
    data = [
        ["Test", 1]
    ]
    df = spark_session.createDataFrame(data, schema)
    output_df = utils.map_column_values(df, dictionary, "Age_ID", "Age")
    assert output_df.first().Age == dictionary.get('1')  # noqa
    assert output_df.select("Age").dtypes[0][1] == "string"


# Test: Check function output if input column is String type
# -------------------------------------------------------------------------

def test_map_string_col_from_dic(spark_session):
    # data schema of input data
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Age_ID", StringType(), True)
        ]
    )
    dictionary = {"1": "a"}
    data = [
        ["Test", "1"]
    ]
    df = spark_session.createDataFrame(data, schema)
    output_df = utils.map_column_values(df, dictionary, "Age_ID", "Age")
    assert output_df.first().Age == dictionary.get('1')  # noqa
    assert output_df.select("Age").dtypes[0][1] == "string"


# Test: Check function output if input column is not in dict
# -------------------------------------------------------------------------

def test_map_null_col_from_dic(spark_session):
    # data schema of input data
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Age_ID", StringType(), True)
        ]
    )
    dictionary = {"2": "a"}
    data = [
        ["Test", "1"]
    ]
    df = spark_session.createDataFrame(data, schema)
    output_df = utils.map_column_values(df, dictionary, "Age_ID", "Age")
    assert output_df.first().Age == None  # noqa


# Test: Check function rases exception if column_name is not in input df
# -------------------------------------------------------------------------

def test_join_to_imd_data_with_no_source_field(spark_session):
    # data schema of input data
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Age_ID", StringType(), True)
        ]
    )
    dictionary = {"1": "a"}
    data = [
        ["Test", "1"]
    ]
    df = spark_session.createDataFrame(data, schema)
    with pytest.raises(Exception) as exc_info:
        output_df = utils.map_column_values(df, dictionary, "Sex_ID", "Age")
        print(str(exc_info.value))
        print(exc_info.value.args[0])
