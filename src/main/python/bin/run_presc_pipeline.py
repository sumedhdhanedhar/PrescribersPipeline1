### Import all the necessary Modules
import os
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date,df_count,df_top10_rec,df_print_schema
import logging
import logging.config
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report,top_5_Prescribers
import pandas

logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    logging.info('main() method is started...')
    try:

        spark= get_spark_object(gav.envn,gav.appName)
        print('Spark object ois created...')
        print(spark)

        get_curr_date(spark)

        for file in os.listdir(gav.staging_dim_city):
            print('file is '+file)
            file_dir =gav.staging_dim_city+ '\\' +file
            print(file_dir)
            if file.split('.')[1]=='csv' :
                file_format='csv'
                header=gav.header
                inferSchema=gav.inferSchema
            elif file.split('.')[1]=='parquet' :
                file_format='parquet'
                header = 'NA'
                inferSchema='NA'

        df_city=load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,inferSchema=inferSchema)

        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')


        for file in os.listdir(gav.staging_fact):
            print('file is '+file)
            file_dir =gav.staging_fact+ '\\' +file
            print(file_dir)
            if file.split('.')[1]=='csv' :
                file_format='csv'
                header=gav.header
                inferSchema=gav.inferSchema
            elif file.split('.')[1]=='parquet' :
                file_format='parquet'
                header = 'NA'
                inferSchema='NA'
        df_fact=load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,inferSchema=inferSchema)
        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact,'df_fact')



        df_city_sel,df_fact_sel=perform_data_clean(df_city,df_fact)

        df_top10_rec(df_city_sel,'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel,'df_fact_sel')

        df_city_final=city_report(df_city_sel,df_fact_sel)
        #df_city_final.show()
        df_top10_rec(df_city_final,'df_city_final')
        df_print_schema(df_city_final,'df_city_final')

        df_presc_final = top_5_Prescribers(df_fact_sel)
        df_top10_rec(df_presc_final,'df_presc_final')
        df_print_schema(df_presc_final,'df_presc_final')





        logging.info('presc_run_pipeline.py is completed..')
    except Exception as exp:
        logging.error('Error in main() method - spark_curr_date(). Please check the stack trace. ' +str(exp),exc_info=True)
        sys.exit(1)



if __name__ == "__main__":
    logging.info('run_presc_pipeline is started...')
    main()