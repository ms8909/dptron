import sys
# from urllib.parse import urlparse,urlsplit
# parsed_uri = urlparse('https://www://stackoverflow.com/www://stackoverflow.com/questions/1234567/blah-blah-blah-blah' )
# #urllib.parse.urlsplit(x)
# import re
#
# myString = "http://example.com/blah"
#
#
# # result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
# print(parsed_uri)
#
# res = ['index']
# abc ="www.mltrons.com"+'/'+'/'.join(res)
# print(abc)
# sys.exit()

from lib.duplication import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib.logs import logger, file_logs
from lib.datetime_formatting import DatetimeFormatting_v2 as DatetimeFormatting
from lib.handle_skewness import HandleSkewness as Skewness

file_logs("mltrons")

s = SparkContext.getOrCreate()


sql = SparkSession(s)


def cleaning_test():
    try:
        df = sql.read.csv("./run/column_rem.csv", inferSchema=True, header=True)

        return_df = Duplication().remove_columns_contains_same_value(df)
        return_df.toPandas().to_csv('./run/rem_test.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(return_df.show())
    except Exception as e:
        logger.error(e)


def remove_cols_containing_nan():
    try:

        logger.debug("this is debug")
        df = sql.read.csv("./run/rem_test.csv", inferSchema=True, header=True)

        return_df = Duplication().remove_columns_containing_all_nan_values(df)
        return_df.toPandas().to_csv('./run/rem_test_result.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(return_df.show())
    except Exception as e:
        logger.error(e)


def date_cleaning():
    try:
        df = sql.read.csv("./run/testing_dates.csv", inferSchema=True, header=True)
        print(df.columns)
        return_df = DatetimeFormatting().date_cleaning(df, ['dates'])
        return_df.toPandas().to_csv('./run/date_test_res.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(return_df.show())
    except Exception as e:
        logger.error(e)


def fetching_specific_columns():
    df = sql.read.csv("./run/date_test_res.csv", inferSchema=True, header=True)

    col_list = Duplication().fetch_columns_containing_url(df)
    print(df.show())
    print("#####################")
    print("resulted_df")
    print(df[col_list].show())


def fetching_datetime_columns():
    df = sql.read.csv("./run/date_test_res.csv", inferSchema=True, header=True)

    col_list = DatetimeFormatting().fetch_columns_containing_datetime(df)
    print(col_list)
    print(df.show())
    print("#####################")
    print("resulted_df")
    print(df[col_list].show())


def chunk_size():
    df = sql.read.csv("./run/file1.csv", inferSchema=True, header=True)
    Duplication().converting_file_into_chunks(df, 100)


def skewness():
    df = sql.read.csv("./run/file1.csv", inferSchema=True, header=True)
    Skewness().remove_skewness(df, ['Purpose'])


remove_cols_containing_nan()
