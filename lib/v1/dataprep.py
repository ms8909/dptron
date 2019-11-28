# imports
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession

# imports custom files
from lib.readfile import *
from lib.logs import logger


class data_prep(object):
    def __init__(self):

        # add variables here
        self.recipe = None
        self.dataframe = None
        self.dataframe_output = None
        self.local = "no"
        self.file_address = ""
        self.file_format = "csv"
        self.output_address = ""
        self.output_format = ""

        # s3 credentials
        self.s3 = {}

        # initializing spark
        self.spark_context = SparkContext().getOrCreate()
        self.spark_session = SparkSession(self.spark_context)

    def prep(self, dataframe=None, s3={}, local="no", file_name="", file_format="csv", output_address="",
             output_format=""):
        """

        :param dataframe:
        :param s3:
        :param local:
        :param file_name:
        :param file_format:
        :param output_address:
        :param output_format:
        :return:
        """

        self.local = local
        self.s3 = s3
        self.file_format = file_format
        self.file_name = file_name
        self.output_address = output_address
        self.output_format = output_format

        """
        convert dataframe into spark dataframe if datarame provided is not none
        """
        try:
            if dataframe != None:
                p = pd.DataFrame()
                if type(dataframe) == type(p):
                    # convert dataframe into spark dataframe
                    self.dataframe = self.spark_session.createDataFrame(dataframe)
                else:
                    self.dataframe = dataframe

            """
            read the file from local or s3 when dataframe passed is none
            """
            if dataframe == None:
                self.dataframe = self.read_as_dataframe()

            """
            time to prepare the recipe
            """
            self.dataframe_output = self.preprocess()

            return self.dataframe_output
        except Exception as e:
            logger.error(e)

    def display_recipe(self):
        """

        :return:
        """
        return True

    def save_recipe(self, address=""):
        """

        :param address:
        :return:
        """
        return True

    def read_as_dataframe(self):
        """

        :return:
        """
        try:
            read = ReadFile()
            self.dataframe = read.read(address=self.file_address, local=self.local, file_format=self.file_format,
                                       s3=self.s3)
            return True
        except Exception as e:
            logger.error(e)

    def save_dataframe(self, address=""):
        """

        :param address:
        :return:
        """
        return True

    def get_recipe(self):
        """

        :return:
        """
        return self.recipe

    def set_recipe(self, recipe=None):
        """

        :param recipe:
        :return:
        """
        self.recipe = recipe
        return True

    def prep_again(self, dataframe=None, s3={}, local_address="", file_format="csv", output_address="",
                   output_format="", recipe=None):
        """

        :param dataframe:
        :param s3:
        :param local_address:
        :param file_format:
        :param output_address:
        :param output_format:
        :param recipe:
        :return:
        """

        return True

    def preprocess(self):
        """

        :return:
        """
        return True
