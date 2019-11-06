import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType


class correct_variable_types(object):
    def __init__(self):
        pass


    def run(self,df):
        
        numeric_columns= self.find_numeric_variables_saved_as_string(df)
        df= self.update_variable_types(df, numeric_columns)
#         self.update_metadata(v)
        return df
        
        try: 
            numeric_columns= self.find_numeric_variables_saved_as_string(df)
            df= self.update_variable_types(df, numeric_columns)
            self.update_metadata(v)
            return df
                
        except Exception as e:
            logger.error(e)
              

    def find_numeric_variables_saved_as_string(self,df):
        """numeric variables"""
        p=df.limit(500).toPandas()

        numeric_columns= []
        columns= self.string_variables(df)  # only take string variables
        for c in columns:
            counter= 0
            e=[]
            float_values= []
            for i in range(500):
                try:
                    v= float(p[c][i])
                    counter= counter+1
                    float_values.append(v)
                except:
#                     print(str(p[c][i]))
                    e.append(str(p[c][i]))

            if len(e)==0:
                numeric_columns.append(c)  
            elif len(set(float_values))<5: 
                if len(set(e))<=1 and len(set(float_values))!=0:
                    numeric_columns.append(c)  
            elif len(float_values)/500*100 > 95:
                numeric_columns.append(c)
        

        return numeric_columns
    
    
    def update_variable_types(self, df, numeric_columns):
        for c in numeric_columns:
            df= df.withColumn(c, col(c).cast(DoubleType()))

        return df
    
    
    def string_variables(self, df):
        s_variables=[]
        for c in df.dtypes:
            if c[1]=="string":
                s_variables.append(c[0])
            
        return s_variables

    def update_metadata(self,column_name=[]):
        
        # update meta data
        
        return True
   
