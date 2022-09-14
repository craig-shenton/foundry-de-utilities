# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           test_dedupe_dataframe.py
DESCRIPTION:    unit-test of 'dedupe_df' utility function
CONTRIBUTORS:   Craig Shenton
CONTACT:        craig.shenton@nhs.net
CREATED:        20 July 2022
VERSION:        0.0.1
"""

# Imports
# -------------------------------------------------------------------------

# Python:
import pytest
import pandas as pd

# Pyspark:
from pyspark.sql import functions as F

# Local:
from project import utils

# Globals
# -------------------------------------------------------------------------

# Test: Check function output is unique
# -------------------------------------------------------------------------


def test_output_cols_are_unique(spark_session):
    # create dataset
    import_date = ['2022-05-24', '2022-05-25']
    date = ['2016-01-01', '2016-01-01']
    code = ['RWJ', 'RWJ']
    age = ['1', '1']
    zipped = list(zip(import_date, date, code, age))
    input_df = pd.DataFrame(zipped, columns=['import_date', 'date', 'code', 'age'])
    input_df = spark_session.createDataFrame(input_df)
    input_df = input_df.withColumn('unique_check', F.concat(F.col('date'), F.col('code'), F.col('age')))
    # test function
    output_df = utils.dedupe_df(input_df, ['date', 'code', 'age'], 'import_date')
    # assertions
    assert pd.Series(output_df.toPandas()['unique_check']).is_unique == True


def test_output_cols_is_latest_value(spark_session):
    # create dataset
    import_date = ['2022-05-24', '2022-05-25']
    date = ['2016-01-01', '2016-01-01']
    code = ['RWJ', 'RWJ']
    age = ['1', '1']
    zipped = list(zip(import_date, date, code, age))
    input_df = pd.DataFrame(zipped, columns=['import_date', 'date', 'code', 'age'])
    input_df = spark_session.createDataFrame(input_df)
    input_df = input_df.withColumn('unique_check', F.concat(F.col('date'), F.col('code'), F.col('age')))
    # Calculate latetest import date
    latest_value = input_df.agg({'import_date': 'max'})
    # test function
    output_df = utils.dedupe_df(input_df, ['date', 'code', 'age'], 'import_date')
    # assertions
    assert output_df.collect()[0]['import_date'] == latest_value.collect()[0]['max(import_date)']
