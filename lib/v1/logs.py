import logging
import datetime
import os
# s_logger = logging.getLogger('py4j.java_gateway')
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()

c_handler.setLevel(logging.DEBUG)

c_handler.setLevel(logging.ERROR)

c_handler.setLevel(logging.INFO)


c_handler.setLevel(logging.WARNING)

# Create formatters and add it to handlers
c_format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                             '%m-%d %H:%M:%S')

c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)


def file_logs(file_name,directory='logs'):
    stream = logging.getLogger(__name__)

    # logger.setLevel(logging.WARNING)

    if not os.path.exists(directory):
        os.makedirs(directory)
    file_name = datetime.datetime.now().strftime('{}_%H_%M_%d_%m_%Y.log'.format(file_name))
    file_name = './{}/{}'.format(directory,file_name)

    f_handler = logging.FileHandler(file_name)
    f_handler.setLevel(logging.ERROR)
    f_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.INFO)
    # f_handler.setLevel(logging.WARNING)

    f_format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                                 '%m-%d %H:%M:%S')
    f_handler.setFormatter(f_format)
    stream.addHandler(f_handler)
