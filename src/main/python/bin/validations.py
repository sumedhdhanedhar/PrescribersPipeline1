import logging
import logging.config
import pandas

#Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF=spark.sql("""select current_date""")
        logger.info("vali9date the spark object by printing current date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error('Name error in method - spark_curr_date(). Please check the stack trace. ' +str(exp),exc_info=True)
        raise
    except Exception as exp:
        logger.error('Error in method - spark_curr_date(). Please check the stack trace. ' +str(exp),exc_info=True)
    else:
        logger.info('spark object is validated. Spark object is ready. ')

def df_count(df,dfName):
    try:
        logger.info(f"Dataframe validation by count df_count() is started from Dataframe {dfName}...")
        df_count=df.count()
        logger.info(f"Dataframe count is {df_count}.")
    except Exception as exp:
        logger.error("Error in method df_count(). Please check the stack trace. "+str(exp))
        raise
    else:
        logger.info("The Dataframe by count df_count() is completed")
def df_top10_rec(df,dfName):
    try:
        logger.info(f"the Datafrakme validation by top 10 record df_top10_rec() is started from Dataframe {dfName}...")
        logger.info("The Dataframe top 10 records are : .")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t'+df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method. please check stack trace : "+ str(exp))
        raise
    else:
        logger.info("Dataframe validation by top 10- records df_top10_rec() is completed")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The dataframe sche,a validation for Dataframe {dfName}...")
        sch=df.schema.fields
        logger.info(f"The Dataframe {dfName} Schema is: ")
        for i in sch:
            logger.info(f'\t{i}')
    except Exception as exp:
        logger.error("Error in the method df_print_schema. Please check the stack trace. "+str(exp))
        raise
    else:
        logger.info("method df_print_schema() is completed")