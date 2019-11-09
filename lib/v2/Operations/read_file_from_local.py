# imports
from pyspark import SparkContext
from pyspark.sql import SparkSession
from ..Logger.logs import logger


class ReadFileFromLocal(object):
    def __init__(self):
        # add variables here
        self.dataframe = ""
        self.address = ''

        # initializing spark
        self.spark_context = SparkContext.getOrCreate()
        self.spark_session = SparkSession(self.spark_context)

    def read(self, address="", file_format="csv"):
        """

        :param address:
        :param file_format:
        :return:
        """
        self.address = address

        if file_format == "csv":
            self.dataframe = self.read_csv(address)
        elif file_format == "excel":
            self.dataframe = self.read_excel(address)
        elif file_format == "parquet":
            self.dataframe = self.read_parquet(address)
        else:
            msg = 'is currently not supported. Please create a feature request on Github'
            logger.debug("File format {} {}".format(file_format, msg))

        return self.dataframe

    def read_csv(self, path):
        """

        :param path:
        :return:
        """
        try:
            self.dataframe = self.spark_session.read.csv(path, inferSchema=True, header=True)
            return self.dataframe
        except Exception as e:
            logger.error(e)
            return {"success": False, "message": e}

    def read_excel(self, path):
        """

        :param path:
        :return:
        """
        try:
            self.dataframe = self.spark_session.read.csv(path, inferSchema=True, header=True)
            return self.dataframe
        except Exception as e:
            logger.error(e)
            return {"success": False, "message": e}

    def read_parquet(self, path):
        """

        :param path:
        :return:
        """
        try:
            self.dataframe = self.spark_session.read.load(path)
            return self.dataframe
        except Exception as e:
            logger.error(e)
            return {"success": False, "message": e}
