from ..imports import *
from textblob import TextBlob


class SpellCorrectionTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column=''):
        super(SpellCorrectionTransformer, self).__init__()
        # lazy workaround - a transformer needs to have these attributes

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

    def removing_special_characters(self, df, col_name):
        """

        :param df:
        :param col_name:
        :return:
        """
        df = df.withColumn(col_name,
                           funct.regexp_replace(funct.trim(funct.lower(funct.col(col_name).cast("string"))),
                                                "[^a-zA-Z0-9]", ""))
        return df

    def apply_spell_correction(self, x):
        """

        :param x:
        :return:
        """
        try:
            res = TextBlob(x).correct()
            print(res)
            return res
        except Exception as e:
            logger.error(e)

    def udf_spell_correction(self):
        """

        :return:
        """

        return funct.udf(lambda x: self.apply_spell_correction(x), StringType())

    def _transform(self, df):
        """

        :param df:
        :return:
        """
        try:
            print("inside transformer")
            col_name = self.getColumn()
            df = self.removing_special_characters(df, col_name)
            df = df.withColumn(col_name, self.udf_spell_correction()(funct.col(col_name)))
            return df

        except Exception as e:
            logger.error(e)
