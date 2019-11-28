from ..imports import *
from ..Logger.logs import logger
import pyspark.sql.functions as funct


class NanToNullConvertor(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='',list_of_col=[]):
        super(NanToNullConvertor, self).__init__()
        #    lazy workaround - a transformer needs to have these attributes

        self._setDefault(column=column)
        self.setColumn(column)
        self.list_of_col = list_of_col

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

    def _transform(self, df):

        cols = [funct.when(
            ~funct.col(x).isin("NULL", 'Nan', 'nan', 'NA', 'na', 'N/A', 'n/a', 'None', 'none', 'NaN', 'nans'),
            funct.col(x)).alias(x) for x in self.list_of_col]
        df = df.select(*cols)
        for i in df.columns:
            df = df.withColumn(i, funct.when((funct.col(i).isNull()), float('nan')).otherwise(funct.col(i)))

        return df
