import sys

from lib.v2.Operations.readfile import ReadFile as read
from lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol
from lib.v2.Middlewares.drop_col_with_same_val import DropSameValueColumn
from lib.v2.Middlewares.fetch_url_columns import FetchUrlCol
from lib.v2.Transformers.url_transformer import UrlTransformer
from lib.v2.Pipelines.etl_pipeline import EtlPipeline
from pyspark.ml import Pipeline
from lib.v2.Logger.logs import file_logs,logger


import pandas as pd
file_logs("mltrons")

res = read.read("./run/traffic_voilation.csv", file_format='csv')

# 845824
# #### reading function
res = read.read("./run/4alan_data_clean.csv", file_format='csv')
res.show()
#
# #### drop null value columns
# drop_col = DropNullValueCol()
# columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
# print(columns_to_drop)
# df = res.drop(*columns_to_drop)
#
# #### drop same value columns
# drop_same_val_col = DropSameValueColumn()
# columns_to_drop = drop_same_val_col.delete_same_val_com(res)
# print(columns_to_drop)
# df = res.drop(*columns_to_drop)
#
# #### url columns cleaned
# etl_pipeline = EtlPipeline()
# etl_pipeline.custom_url_transformer(res)
# res = etl_pipeline.transform(res)
#
# ####
#
#
# #### datetime  columns split
# etl_pipeline = EtlPipeline()
# etl_pipeline.custom_date_transformer(res)
# res = etl_pipeline.transform(res)
# res.toPandas().to_csv("check1.csv")
# ####
#
#
# #### fill missing values  columns split
# etl_pipeline = EtlPipeline()
# etl_pipeline.custom_filling_missing_val(res)
# res = etl_pipeline.transform(res)
# res.toPandas().to_csv("check2.csv")
# ####


#### fill skewed_columns
etl_pipeline = EtlPipeline()
res = res.select([ 'Latitude'])
etl_pipeline.custom_skewness_transformer(res)

res = etl_pipeline.transform(res)
res.toPandas().to_csv("check3.csv")
####
