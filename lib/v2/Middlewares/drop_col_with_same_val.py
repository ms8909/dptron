from ..imports import *


class DropSameValueColumn(object):

    def __init__(self):
        self.variables = []
        self.categorical_variables = []
        self.numerical_variables = []
        self.models = {}
        self.df = None

    def run(self, df, existed_variables=[]):

        """
        remove columns which contains only one kind of value
        :param df: original dataframe containing data
        :return: return dataframe after removing columns
        """
        
        columns= existed_variables
        
        try:

            col_counts = \
                df.select([(funct.countDistinct(funct.col(col))).alias(col) for col in columns]).collect()[
                    0].asDict()
            to_drop = [k for k, v in col_counts.items() if v == 1]

            logger.warn("Columns to drop")
            logger.warn(to_drop)

            return to_drop
        except Exception as e:
            logger.error(e)
            return []
