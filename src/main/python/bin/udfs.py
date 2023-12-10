from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *

@udf(returnType=IntegerType())
def column_split_cnt(column):
    return len(column.split(' '))