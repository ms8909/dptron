from pyspark import SparkContext
from pyspark.sql import SparkSession
from ..Logger.logs import logger


# imports


class ReadFileFromS3(object):
    def __init__(self, address, file_format, s3):
        # add variables here
        self.s3 = {}
        self.dataframe = ''
        self.address = ''
        self.spark_context = ''
        self.hadoop_conf = ''
        self.spark_session = ''

    def read(self, address="", file_format="csv", s3={}):
        """

        :param address:
        :param file_format:
        :param s3:
        :return:
        """
        try:
            status, message = self.init_spark_with_s3(s3)
            if status is False:
                return {"success": status, "message": message}

            self.s3 = s3
            self.address = address

            if file_format == "csv":
                self.dataframe = self.read_csv(address)
            elif file_format == "excel":
                self.dataframe = self.read_excel(address)
            elif file_format == "parquet":
                self.dataframe = self.read_parquet(address)
            else:
                print(
                    "File format " + file_format + " is currently not supported. Please create a feature request on Github")

            return self.dataframe
        except Exception as e:
            logger.error(e)

    def read_csv(self, path):
        """

        :param path:
        :return:
        """
        try:
            self.dataframe = self.spark_session.read.csv('s3n://' + self.s3["bucket"] + '/' + path, inferSchema=True,
                                                         header=True)
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
            self.dataframe = self.spark_session.read.csv('s3n://' + self.s3["bucket"] + '/' + path, inferSchema=True,
                                                         header=True)
            return self.dataframe
        except Exception as e:
            logger.error(e)
            return {"success": False, "message": e}

    def read_parquet(self, path):
        try:
            self.dataframe = self.spark_session.read.load('s3n://' + self.s3["bucket"] + '/' + path)
            return self.dataframe
        except Exception as e:
            logger.error(e)
            return {"success": False, "message": e}

    def init_spark_with_s3(self, s3):
        """

        :param s3:
        :return:
        """

        success = False
        message = ""

        for i in range(10):

            try:

                self.spark_context = SparkContext().getOrCreate()

                self.hadoop_conf = self.spark_context._jsc.hadoopConfiguration()
                self.hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                self.hadoop_conf.set("fs.s3n.awsAccessKeyId", s3["ID"])
                self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", s3["key"])

                self.spark_session = SparkSession(self.spark_context)
                success = True
                continue
            except Exception as e:
                logger.error(e)
                message = e
                pass

        return success, message
