from ..imports import *


class FetchUrlCol(object):
    def __init__(self):
        pass

    @staticmethod
    def fetch_columns_containing_url(df,existed_variables=[]):
        """
        Automatically fetch column name contains urls
        :param df: orig dataframe
        :return: return list of columns containing urls
        """
        columns = existed_variables
        try:
            col_dict = df.select([funct.col(col).rlike(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))').alias(col) for col in
                                  columns]).collect()[0].asDict()
            col_containing_url = [k for k, v in col_dict.items() if v is True]
            return col_containing_url
        except Exception as e:
            logger.error(e)
            return []
