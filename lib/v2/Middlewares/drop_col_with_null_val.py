from ..imports import *


class DropNullValueCol(object):

    def __init__(self):
        self.variables = []
        self.categorical_variables = []
        self.numerical_variables = []
        self.models = {}
        self.df = None

    def get_variables_types(self):
        """numeric variables"""
        variables = self.df.dtypes
        self.variables = variables

        return variables

    @staticmethod
    def delete_var_with_null_more_than(df, threshold=30):
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
                     df.columns]).collect()[0].asDict()
            size_df = df.count()
            to_drop = [k for k, v in null_counts.items() if ((v / size_df) * 100) >= threshold]
            logger.warn("columns to drop :")
            logger.warn(to_drop)

            return to_drop

        except Exception as e:
            logger.error(e)
            return []