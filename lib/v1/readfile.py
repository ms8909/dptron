# imports
from lib.read_file_from_local import ReadFileFromLocal
from lib.read_file_from_s3 import ReadFileFromS3
from lib.logs import logger


class ReadFile(object):
    def __init__(self):
        # add variables here
        self.dataframe = ""

    def read(self, address="", local="yes", file_format="csv", s3={}):
        """

        :param address:
        :param local:
        :param file_format:
        :param s3:
        :return:
        """
        try:
            if local == "yes":
                """
                Time to read the file saved locally
                """
                rf = ReadFileFromLocal()
                self.dataframe= rf.read(address, file_format)

            elif s3 != {}:
                """
                Time to read data from s3
                """
                self.dataframe = ReadFileFromS3(address, file_format, s3)

            else:
                """
                Not sure where the file is saved.
                """
                message = "Please make sure you have file saved on either your local system or s3."
                logger.debug(message)
                self.dataframe = {"success": False, "message": message}

            return self.dataframe
        except Exception as e:
            logger.error(e)
