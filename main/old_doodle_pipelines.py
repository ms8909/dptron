import sys


from lib.v2.columns_dropping_nan import ColumnsDroppingContainsNan
from lib.v2.columns_dropping_with_same_value import ColumnsDroppingSameValue
from lib.v2.removing_duplication_urls import RemovingDuplicationUrl
from lib.v2.datetime_formatting import DatetimeFormatting
from lib.v2.minimize_skewness import MinimizeSkewness
from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib_v2.logs import logger, file_logs
from pyspark.ml import Pipeline

file_logs("mltrons")

s = SparkContext.getOrCreate()

sql = SparkSession(s)


def columns_same_value():
    try:
        df = sql.read.csv("./run/rem_test.csv", inferSchema=True, header=True)

        columns_with_same_val = ColumnsDroppingSameValue()
        model = Pipeline(stages=[columns_with_same_val]).fit(df)
        result = model.transform(df)

        result.toPandas().to_csv('./run/pipeline_same_value.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(result.show())
    except Exception as e:
        logger.error(e)


def remove_cols_containing_nan():
    try:

        logger.debug("this is debug")
        df = sql.read.csv("./run/column_rem.csv", inferSchema=True, header=True)

        col_contains_nan = ColumnsDroppingContainsNan()
        model = Pipeline(stages=[col_contains_nan]).fit(df)
        result = model.transform(df)

        result.toPandas().to_csv('./run/pipeline_nan_value.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(result.show())

    except Exception as e:
        logger.error(e)


def remove_url_duplication():
    df = sql.read.csv("./run/date_test_res.csv", inferSchema=True, header=True)

    url_duplication = RemovingDuplicationUrl()
    model = Pipeline(stages=[url_duplication]).fit(df)
    result = model.transform(df)

    result.toPandas().to_csv('./run/pipeline_url.csv')

    print("resulted_df")
    print(result.show())


def date_conversion():
    df = sql.read.csv("./run/date_test_res.csv", inferSchema=True, header=True)
    datetime_formatting = DatetimeFormatting()
    model = Pipeline(stages=[datetime_formatting]).fit(df)
    res = model.transform(df)
    print("resulted_df")
    print(res.show())


def remove_skewness():
    df = sql.read.csv("./run/file1.csv", inferSchema=True, header=True)

    min_skewness = MinimizeSkewness(['Purpose'])
    model  = Pipeline(stages=[min_skewness]).fit(df)
    res = model.transform(df)
    print(res.show())


if __name__ == "__main__":
    columns_same_value()
    # remove_cols_containing_nan()
    # remove_url_duplication()
    # date_conversion()
    # remove_skewness()