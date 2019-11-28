from ..imports import *


class FetchDateTimeCol(object):
    def __init__(self):
        self.variables = []
        self.categorical_variables = []
        self.numerical_variables = []
        self.models = {}
        self.df = None

    @staticmethod
    def find_datetime_features(df, existed_variables=[]):
        """
        Automatically detects the column which contains the date values
        :param df: orig dataframe
        :return: list of column name contains the date values
        """
        columns = existed_variables

        if len(existed_variables) < 1:
            columns = df.columns
        try:
            col_dict = df.select([funct.col(col).rlike(r'(\d+(/|-){1}\d+(/|-){1}\d{2,4})').alias(col) for col in
                                  columns]).collect()[0].asDict()
            col_containig_dates = [k for k, v in col_dict.items() if v is True]

            logger.warn("columns cotanins date:")
            logger.warn(col_containig_dates)

            return col_containig_dates
        except Exception as e:
            logger.error(e)
            return []
