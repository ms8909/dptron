# imports
from lib.logs import logger

class saveFileS3(object):
    def __init__(self):
        # add variables here
        self.temp = ""

    def save(self, address="", file_format="csv", s3={}):
        """

        :param address:
        :param file_format:
        :param s3:
        :return:
        """
        self.s3 = s3

        return True

    def save_csv(self):
        """

        :return:
        """
        return True

    def save_excel(self):
        """

        :return:
        """
        return True

    def save_parquet(self):
        """

        :return:
        """
        return True
