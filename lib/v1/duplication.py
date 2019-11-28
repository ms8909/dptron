import re
import sys
from urllib.parse import urlparse
import pyspark.sql.functions as funct
from nltk.corpus import stopwords
from pyspark.sql.types import StringType
from lib.logs import logger
from pyspark.sql import Window


class Duplication(object):

    def __init__(self):
        """

        """
        self.stop_words = stopwords.words('english')

    @staticmethod
    def remove_duplicate_ids(df, column_name=[]):
        """
        Remove the special characters from the id columns to remove duplicates
        :param df: orig dataframe
        :param column_name: list of columns containing id
        :return: dataframe with new columns after cleaning id
        """
        if column_name == []:
            column_name = df.columns
        for i in column_name:
            df = df.withColumn(i + '_new',
                               funct.regexp_replace(funct.trim(funct.lower(funct.col(i).cast("string"))),
                                                    "[^a-zA-Z0-9]", ""))
        return df

    def removing_stop_words(self, x, base_url):
        """
        url column and base_url is given and cleaned url is returned

        :param x: row on which cleaning is need to be performed
        :param base_url: Contains base_url
        :return: cleaned url
        """
        try:
            # If base_url param is empty figure out base_url using urllib
            if base_url == '':
                base_url = urlparse(x)
                base_url = base_url.netloc if base_url.scheme != '' else base_url.path.split("/")[0]
            x = x.replace("https://", "").replace("http://", "").replace(base_url, "")

            # fetch only alphabets ignore all special characters
            tokens = re.findall(r"[\w:']+", x)

            # remove duplicate words from url
            tokens = list(dict.fromkeys(tokens))

            # remove stop words from url
            elem = [word for word in tokens if word not in self.stop_words]

            # add base_url to the url
            elem.insert(0, base_url)

            return '/'.join(elem)
        except Exception as e:
            logger.error(e)

    def udf_remove_stop_words(self, base_url):
        """
        a run function to create a udf function with default params
        :param base_url: contains base_url if any
        :return: cleaned url
        """
        return funct.udf(lambda x: self.removing_stop_words(x, base_url))

    def remove_duplicate_urls(self, df, column_name, base_url=''):
        """
        Orig dataframe is received  with columns containing urls .
        Those columns are cleaned to remove duplication

        :param df: dataframe containing data which need to be cleaned
        :param column_name: list of columns containing urls
        :param base_url: base_url optional
        :return: return dataframe with _new column name append describing cleaned column
        """
        try:
            for i in column_name:
                df = df.withColumn(i + '_new',
                                   self.udf_remove_stop_words(base_url)(
                                       funct.trim(funct.lower(funct.col(i).cast("string")))))

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def remove_columns_containing_all_nan_values(df, threshold=80):
        """
        receives a dataframe and threshold,
        removes column which contains nan >=threshold
        :param df: original dataframe containing data
        :param threshold: nans threshold from 0-100 as percentage
        :return: return dataframe after removing columns
        """
        try:
            null_counts = \
                df.select(
                    [funct.count(funct.when(funct.col(col).isNull() |
                                            funct.col(col).contains("NULL") |
                                            funct.col(col).contains("null") |
                                            funct.col(col).contains("Null") |

                                            funct.col(col).contains("None") |
                                            funct.col(col).contains("NONE") |
                                            funct.col(col).contains("none"), col)).alias(col) for col in
                     df.columns]).collect()[
                    0].asDict()
            size_df = df.count()
            to_drop = [k for k, v in null_counts.items() if ((v / size_df) * 100) >= threshold]
            print("check")
            logger.warn("columns to drop ")
            logger.warn(to_drop)
            df = df.drop(*to_drop)
            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def remove_columns_contains_same_value(df):
        """
        remove columns which contains only one kind of value
        :param df: original dataframe containing data
        :return: return dataframe after removing columns
        """
        try:

            col_counts = df.select([(funct.countDistinct(funct.col(col))).alias(col) for col in df.columns]).collect()[
                0].asDict()
            to_drop = [k for k, v in col_counts.items() if v == 1]

            df = df.drop(*to_drop)

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def fetch_columns_containing_url(df):
        """
        Automatically fetch column name contains urls
        :param df: orig dataframe
        :return: return list of columns containing urls
        """
        try:
            col_dict = df.select([funct.col(col).rlike(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))').alias(col) for col in
                                  df.columns]).collect()[0].asDict()
            col_containing_url = [k for k, v in col_dict.items() if v is True]
            return col_containing_url
        except Exception as e:
            logger.error(e)

    def converting_file_into_chunks(self, df, chunk_size=100):
        """
        Large pyspark dataframe is converted into chunks and converted to pandas to
         convert it into pandas df
        :param df: orig dataframe
        :param chunk_size: size of each dataframe chunk
        :return:
        """
        # created window using first column
        window = Window.orderBy(funct.col(df.columns[0]))
        df = df.withColumn('row_number', funct.row_number().over(window))

        for i in range(0, df.count(), chunk_size):
            chunk = df.where((funct.col('row_number') >= i) & (funct.col('row_number') < (i + chunk_size)))
            logger.info(chunk.count())
            if chunk.count() != 0:
                pd_df = chunk.toPandas()

                ### you can do what ever you want to with the pandas ddataframe
                pd_df.to_csv("{}_file.csv".format(i))
                print("############")
