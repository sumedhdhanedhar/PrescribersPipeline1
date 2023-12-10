from pyspark.sql import SparkSession
import logging
import logging.config

#Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_spark_object(envn,appName):
    try:
        logger.info(f"get_spark_object() method is started.The '{envn}' envn is used")
        if envn=='TEST':
            master='local'
        else:
            master='yarn'
        spark=SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()

    except NameError as exp:
        logger.error('Name error in method - get_spark_object(). Please check the stack trace. ' +str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error('Error in method - get_spark_object(). Please check the stack trace. ' +str(exp), exc_info=True)
    else:
        logger.info('spark object is created')
    return spark
