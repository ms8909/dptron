from ..imports import *


class TypeDoubleTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):

    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='',list_of_col=[]):
        super(TypeDoubleTransformer, self).__init__()

        self._setDefault(column=column)
        self.setColumn(column)
        self.list_of_col =list_of_col
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

        ####

        """
        old one
        return df.withColumn(self.getColumn(), funct.col(self.getColumn()).cast(DoubleType()))
        """
        for i in self.list_of_col:
            df = df.withColumn(i, funct.regexp_replace(funct.col(i), "[^a-zA-Z0-9.]", ""))
            df = df.withColumn(i, df[i].cast(DoubleType()))
        return df

