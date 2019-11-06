from lib.v2.imports import *


class SkewnessTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='', threshold=0.7):
        super(SkewnessTransformer, self).__init__()
        # lazy workaround - a transformer needs to have these attributes

        self._setDefault(column=column)
        self.setColumn(column)
        self.threshold = threshold

    def getColumn(self):
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.column)

    def setColumn(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(column=value)

    @staticmethod
    def handle_neg_values(df, column_name):
        """
        Minimum value of columns is fetched
        zero/negative values are handled to remove skewness
        :param df: orig dataframe
        :param column_name: column_name
        :return: updated column to remove skewness
        """
        try:
            min_value = df.agg({column_name: "min"}).collect()[0][0]
            min_value = int(min_value)
            logger.warn("min_value is {}".format(min_value))
            if min_value <= 0:
                df = df.withColumn(column_name, funct.col(column_name) + abs(min_value) + 0.01)
            return df
        except Exception as e:
            logger.error(e)
    
    @staticmethod
    def apply_box_cox(x):
        """
        boxcox function is applied to remove skewness
        :param x: row wise values of a column
        :return: updated value with minimum skewness
        """

        return float(round(stats.boxcox([x])[0][0], 2))

    @staticmethod
    def apply_log(x):
        """
        log function is applied to remove skewness
        :param x: row wise values of a column
        :return: updated value with minimum skewness
        """
        try:
            return float(round(np.log(x), 3))
        except Exception as e:
            logger.error(e)

    def udf_box_cox(self):
        """
        udf function to call boxcox
        :return: less skewed values
        """
        return funct.udf(lambda x: self.apply_box_cox(x), FloatType())

    def udf_log(self):
        """
        udf function to call log function
        :return:less skewed values
        """
        return funct.udf(lambda x: self.apply_log(x), FloatType())

    def _transform(self, df):
        """
        skewness is minimized using boxcox and log functions
        if skewness is negative boxcox is used to remove skewness
        if skewness is positive log is used to remove skewness
        if skewness is less then threshold remove skewness is not called

        :param df:orig dataframe
        :param columns: list of columns on which skewness is needed to removed
        :return: less skewed dataframe
        """
        try:
            col_name = self.getColumn()
            skew_val = df.select(funct.skewness(df[col_name])).collect()[0][0]
            if skew_val is not None:
                if abs(skew_val) > self.threshold and skew_val < 0:

                    df = self.handle_neg_values(df, col_name)
                    df = df.withColumn(col_name, self.udf_box_cox()(funct.col(col_name)))

                elif abs(skew_val) > self.threshold and skew_val > 0:
                    df = self.handle_neg_values(df, col_name)
                    df = df.withColumn(col_name, self.udf_log()(funct.col(col_name)))

            return df
        except Exception as e:
            logger.error(e)
            return df
