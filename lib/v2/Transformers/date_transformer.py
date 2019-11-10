from ..imports import *
from ..Logger.logs import logger
import pyspark.sql.functions as funct
from dateutil import parser


class DateTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='', time_format='yyyy-mm-dd HH:mm:ss'):

        super(DateTransformer, self).__init__()
        #    lazy workaround - a transformer needs to have these attributes

        self._setDefault(column=column)
        self.setColumn(column)
        self.time_format = time_format

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

        time_variable = self.getColumn()
        new_time_variable = time_variable + '_new'

        # code from tawab. Convert all times in a same format.

        df = df.withColumn(new_time_variable, self.udf_date_formatting()(funct.col(time_variable).cast("String")))
        df = df.withColumn(new_time_variable,
                           funct.from_unixtime(funct.unix_timestamp(new_time_variable, self.time_format)).cast(
                               TimestampType()))

        df = df.withColumn(time_variable + '_year', funct.year(new_time_variable))
        df = df.withColumn(time_variable + '_month', funct.month(new_time_variable))
        df = df.withColumn(time_variable + '_day', funct.dayofmonth(new_time_variable))
        df = df.withColumn(time_variable + '_dayofweek', funct.dayofweek(new_time_variable))
        df = df.withColumn(time_variable + '_hour', funct.hour(new_time_variable))
        df = df.withColumn(time_variable + '_minutes', funct.minute(new_time_variable))
        df = df.withColumn(time_variable + '_seconds', funct.second(new_time_variable))

        df = df.drop(new_time_variable)
        df = df.drop(time_variable)
        return df

    def date_formatting(self, x):
        """
        dateutill library is used to convert the different format of dates into standard format
        :param x: row wise date values
        :return: standard format of date
        """
        try:
            return str(parser.parse(x))
        except Exception as e:
            logger.error(e)
            return str(x)

    def udf_date_formatting(self):
        """
        Run function calls the main date_formatting function
        :return: standard format of date
        """
        
        return funct.udf(lambda row: self.date_formatting(row))
