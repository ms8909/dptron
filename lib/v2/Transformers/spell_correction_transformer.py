from ..imports import *
from pattern.en import suggest

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
                                                "[^a-zA-Z0-9]", " "))
        return df

    def apply_spell_correction(self, x):
        """

        :param x:
        :return:
        """
        try:
            pattern = re.compile(r"(.)\1{2,}")
            list_of_elem = x.split(" ")
            clean_x = [pattern.sub(r"\1\1", i) for i in list_of_elem]
            suggest_val = [suggest(i)[0][0] for i in clean_x]
            return " ".join(suggest_val)

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
            col_name = self.getColumn()
            df = self.removing_special_characters(df, col_name)
            df = df.withColumn(col_name, self.udf_spell_correction()(funct.col(col_name)))
            return df

        except Exception as e:
            logger.error(e)
