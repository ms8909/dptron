import sys

from lib.v2.Operations.readfile import ReadFile as rf
from lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol
from lib.v2.Middlewares.drop_col_with_same_val import DropSameValueColumn
from lib.v2.Middlewares.fetch_url_columns import FetchUrlCol
from lib.v2.Transformers.url_transformer import UrlTransformer
from lib.v2.Pipelines.etl_pipeline import EtlPipeline
from pyspark.ml import Pipeline
from lib.v2.Logger.logs import file_logs, logger
from pattern.en import suggest
from lib.v2.imports import *
import pandas as pd
file_logs("mltrons")




# res = rf.read(address="./run/dataset/test.csv", local="yes", file_format="csv", s3={})
# df = res.withColumn('res', res['res'].cast(DoubleType()))
# df.show()
# sys.exit()

from textblob import TextBlob
from spellchecker import SpellChecker
from autocorrect import spell

# print(spell('yelow'))
# spells = SpellChecker()
#
# print(spells.correction("hte"))

res = rf.read(address="./run/dataset/spell.csv", local="yes", file_format="csv", s3={})
res.show()

etl_pipeline = EtlPipeline()
etl_pipeline.custom_spell_transformer(res,['col1','col2'])
res2 = etl_pipeline.transform(res)
print("columns are printing")
print(res2.columns)
res2.show()
sys.exit()
# res = rf.read(address="./run/dataset/rollingsales_Manhattan.csv", local="yes", file_format="csv", s3={})

drop_col = DropNullValueCol()
columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
res = res.drop(*columns_to_drop)
print("columns are printing")
print(res.columns)

drop_same_val_col = DropSameValueColumn()
columns_to_drop = drop_same_val_col.delete_same_val_com(res)
res = res.drop(*columns_to_drop)
print("columns are printing")
print(res.columns)

etl_pipeline = EtlPipeline()
etl_pipeline.custom_url_transformer(res)
res = etl_pipeline.transform(res)
print("columns are printing")
print(res.columns)

etl_pipeline = EtlPipeline()
etl_pipeline.custom_date_transformer(res)
res = etl_pipeline.transform(res)
print("columns are printing")
print(res.columns)
res.write.csv('./run/testing/rollingsales_Manhattan_before_filling3.csv', header=True)


etl_pipeline = EtlPipeline()
etl_pipeline.custom_filling_missing_val(res)
res = etl_pipeline.transform(res)
print("columns are printing")
print("columns before skewnesss")
print(res.columns)


res.write.csv('./run/testing/rollingsales_Manhattan_without_skew_new3.csv', header=True)
etl_pipeline = EtlPipeline()
etl_pipeline.custom_skewness_transformer(res)
res = etl_pipeline.transform(res)
print("columns after skewness")
print("columns are printing")
print(res.columns)

res.write.csv('./run/testing/rollingsales_Manhattan_clean91.csv', header=True)
