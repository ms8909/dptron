import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger
from pyspark.sql.functions import *



class split_time_variable(object):
    def __init__(self):
        pass


    def run(self,df):
        
        try: 
            time_variables= self.find_time_variables(df)
            for v in time_variables:
                df= self.make_new_variables(df, v)
                self.update_metadata(v)
            return df
                
        except Exception as e:
            logger.error(e)
            print(e)
            return(e)
              

    def find_time_variables(self,df):
        variables= df.dtypes
        
        time_variables=[]
        for v in variables:
            if v[1]=="timestamp":
                time_variables.append(v[0])
        
        return time_variables
    
    
    def make_new_variables(self, df, v):
        df= df.withColumn(v+'_year', year(v))
        df= df.withColumn(v+'_month', month(v))
        df= df.withColumn(v+'_day', dayofmonth(v))
        df= df.withColumn(v+'dayofweek', dayofweek(v))
        df= df.withColumn(v+'hour', hour(v))
        df= df.withColumn(v+'min', minute(v))
        df= df.withColumn(v+'sec', second(v))
        df= df.drop(v)

        
        # also add day of week
        
        return df

    def update_metadata(self,column_name=[]):
        
        # update meta data
        
        return True
   
