from sys import argv as dbg_sys_argv

dbg_sys_argv.extend(['--cfg_file_path', 's3://slf-ca-dev-glue/amp/test/testing_schema/config.json'])
from urllib.parse import urlparse
import boto3
from awsglue.transforms import *
import random
import os
from pyspark.sql.functions import col, lit, when
from datetime import date, datetime
from pyspark.sql.functions import col, lit, to_date
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
import sys
import json
import time
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pyspark.sql.functions as func



class generate_testdata():
    def __init__(self):

        self.glueContext = None
        self.json_obj = None
        self.new_json_obj = None
        self.df_readschema = None
        self.sqlContext = None
        self.df_tgt = None
        self.spark = None
        self.df_customer_table = None
        self.df_order_table = None

    def initialize(self):
        spark_context = SparkContext()
        self.glueContext = GlueContext(spark_context)
        self.sqlContext = SQLContext(spark_context)
        self.spark = self.glueContext.spark_session

            

    def extract(self):
        args_new = getResolvedOptions(sys.argv, ['cfg_file_path_new'])
        cfg_file_path_new = args_new['cfg_file_path_new']

        file_res = urlparse(cfg_file_path_new)
        s3_resource = boto3.resource('s3')
        file_obj = s3_resource.Object(file_res.netloc, file_res.path.lstrip('/'))
        content = file_obj.get()['Body'].read()
        content_new = content
        self.new_json_obj = json.loads(content_new)
        print('===================================Second Json=======================================')
        print(content_new)
        self.df_order_table = self.glueContext.create_dynamic_frame_from_options(connection_type='s3',
                                                                           connection_options={
                                                                               "paths": [self.new_json_obj[
                                                                                             'Schema'][
                                                                                             'src_file_path']]
                                                                           },
                                                                           format='csv',
                                                                           format_options={
                                                                               "withHeader": True}
                                                                           ).toDF()
        self.df_customer_table= self.glueContext.create_dynamic_frame_from_options(connection_type='s3',
                                                                             connection_options={
                                                                                 "paths": [self.new_json_obj[
                                                                                               'Schema'][
                                                                                               'src_file_path_2']]
                                                                             },
                                                                             format=self.new_json_obj[
                                                                                               'Schema'][
                                                                                               'format'],
                                                                             format_options={
                                                                                 "withHeader": True}
                                                                             ).toDF()

        self.df_order_table.show()
        self.df_customer_table.show()
        

    def transform(self):
        self.df_order_table = self.df_order_table.withColumn('order_date', to_date(col('order_datetime'), 'yyyy-MM-dd'))
        self.df_order_table = self.df_order_table.withColumn('order_month', func.month(col('order_datetime')))


        df_filter_cust=self.df_customer_table.where(col('age')>18)
        ###inner join
        df_order_customer= self.df_order_table.join(df_filter_cust,
                                      on=(self.df_order_table['customer_id'] == df_filter_cust['customer_id']),
                                      how='inner').select(df_filter_cust['customer_id'], self.df_order_table['order_id'],
                                                          self.df_order_table['order_month'],self.df_order_table['amount'])

        # total sales amount for each month of each customer who are greater than age 18
        wind = Window.partitionBy('customer_id','order_month')

        df_order_customer = df_order_customer.withColumn('total_sale', func.sum(col('amount')).over(wind))

        df_order_customer.distinct()
        df_order_customer.show()

        ###list the cutomer_id and their second order_id of customers who places more than 2 order in last 20 dayssss
        ########################
        wind = Window.partitionBy('customer_id','order_date').orderBy(func.col( 'order_id' ).asc() )
        df_temp = self.df_order_table.withColumn('row', func.row_number().over(wind))\


        df_temp=df_temp.withColumn('current_date', to_date(func.current_timestamp(), 'yyyy-MM-dd'))

        df_temp=df_temp.withColumn('diff_days', func.datediff('current_date', 'order_date'))


        df_temp=df_temp.withColumn("diff",when((col('diff_days')<=lit(20)),lit(1))
                                   .otherwise(0))
        df_temp=df_temp.where(col('diff')==1)
        wind = Window.partitionBy('customer_id')
        df_temp = df_temp.withColumn('count', func.count('order_id').over(wind))
        df_temp=df_temp.where((col('count')>2) & (col('row')==2))

        df_temp.show()


    def load(self):
        # self.df_tgt.coalesce(1)
        # self.df_tgt.write.mode('Overwrite').format('csv').save(
        #     's3 path',
        #     header='true')
        dynf = DynamicFrame.fromDF(self.df_tgt, self.glueContext,"glue_job")
        self.glueContext.write_dynamic_frame_from_options(frame=dynf,
                                                           connection_type="s3",
                                                           connection_options={"path": 's3://slf-ca-dev-glue/amp/test/testing_schema/output',

                                                                               },
                                                            format="parquet",
                                                           format_options={},
                                                           transformation_ctx="")




def main():
    the_job = generate_testdata()
    the_job.initialize()
    the_job.extract()
    the_job.transform()
    the_job.load()


if __name__ == '__main__':
    main()

