from ..imports import *

class ChangeColumnsOrder(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column=[]):
        super(ChangeColumnsOrder, self).__init__()

        # self._setDefault(column=column)
        # self.setColumn(column)
        self.list_of_columns = column

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
        return df.select(self.list_of_columns)
