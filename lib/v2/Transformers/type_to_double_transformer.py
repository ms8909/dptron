from lib.v2.imports import *


class TypeDoubleTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):

    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column=''):
        super(TypeDoubleTransformer, self).__init__()

        self._setDefault(column=column)
        self.setColumn(column)

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
        return df.withColumn(self.getColumn(), funct.col(self.getColumn()).cast(DoubleType()))


