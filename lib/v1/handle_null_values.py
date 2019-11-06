import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType
import datawig
import datetime




#template only


class handle_null_values(object):
    def __init__(self):
        self.variables=[]
        self.categorical_variables=[]
        self.numerical_variables=[]
        self.models={}
        self.df=None


    def run(self,df):
        
        try:
            self.df=df
            variables= self.get_variables_types()

            df= self.train_model_for_categorical_variables()
            df= self.impute_categorical_variables()
            del_variables= self.delete_extra_categorical_variables()
            
            self.update_metadata(v)
                
#             df= self.train_model_for_numerical_variables()
#             df= self.impute_numerical_variables()
#             del_variables= self.delete_extra_numerical_variables()
            
            
            return df
                
        except Exception as e:
            logger.error(e)
              

    def get_variables_types(self):
        """numeric variables"""
        variables=self.df.dtypes
        self.variables= variables

        return variables
    
    
    def delete_var_with_null_more_than(self, dataframe=None, percentage=30):
        """numeric variables"""

        if dataframe == None:
            dataframe= self.df
            
        total_rows= dataframe.count()
        all_columns= dataframe.columns

        dropped_columns = []

        describe_dataframe= dataframe.select([count(when(col(c).isNull() |
                                                         col(c).contains("NULL") |
                                                         col(c).contains("null")
                                                         | col(c).contains("None") |
                                                         col(c).contains("NONE") |
                                                         col(c).contains("none"), c)).alias(c) for c in dataframe.columns]).toPandas()

        for c in all_columns:
            missing_values= int(describe_dataframe[c][0])

            percent_missing= (missing_values)/total_rows *100

            if percent_missing>percentage:
                dropped_columns.append(c)

        return dropped_columns
    
  

    
    
    def train_model_for_categorical_variables(self):
        time= str(datetime.datetime.now())
        for c in variables:
            if c[1]=="string":
                
                var= self.variables.copy()
                var.remove(c)
                #initialize the model

                imputer = datawig.SimpleImputer(
                    input_columns=var, # column(s) containing information about the column we want to impute
                    output_column=c, # the column we'd like to impute values for
                    output_path = 'lib/imputer_models'+time+'/'+str(c) # stores model data and metrics
                    )
                imputer.fit(train_df=self.df, num_epochs=5)
                self.models[c]= imputer
                self.categorical_variables.append(c[0])
                
                print("Training completed to treat the categorical variable: ",c[0])
                
        return True
    
    
    def impute_categorical_variables(self, df=None):
        if df==None:
            df= self.df
            
        xyz= df
        for c in self.categorical_variables:
            m= self.models[c]
            xyz= m.predict(xyz)
                
        return True
    
    def impute_numerical_variables(self, df=None):
        if df==None:
            df= self.df
            
        xyz= df
        for c in self.numerical_variables:
            m= self.models[c]
            xyz= m.predict(xyz)
                
        return True
    
    
    def train_model_for_numerical_variables(self, df, variables):
        time= str(datetime.datetime.now())
        for c in variables:
            if c[1]!="string":
                
                var= self.variables.copy()
                var.remove(c)
                #initialize the model

                imputer = datawig.SimpleImputer(
                    input_columns=var, # column(s) containing information about the column we want to impute
                    output_column=c, # the column we'd like to impute values for
                    output_path = 'lib/imputer_models'+time+'/'+str(c) # stores model data and metrics
                    )
                imputer.fit(train_df=df, num_epochs=5)
                self.models[c]= imputer
                self.numerical_variables.append(c[0])
                
                print("Training completed to treat the numerical variable: ",c[0])
                
        return True
    
   

    def delete_extra_categorical_variables(self, df=None, variables=None):
        self.df= df
        
        if variables==None:
            varaibles= self.categorical_variables
        
        #make a list of all the categorical columns. 
        no_n=[ 'Unnamed: 0']   

        #delete all unnecessary columns, including the one that make no senese.
        for cv in varaibles:
            no_n.append(cv+'_imputed_proba')
            no_n.append(cv)

        df= df.drop(no_n, axis=1)
        
        self.df=df

        return self.df

    
    def delete_extra_numerical_variables(self, df=None, variables=None):
        if df==None:
            df= self.df
        
        if variables==None:
            varaibles= self.numerical_variables
        
        #make a list of all the categorical columns. 
        no_n=[ 'Unnamed: 0']   

        #delete all unnecessary columns, including the one that make no senese.
        for nv in varaibles:
            no_n.append(nv+'_imputed_proba')
            no_n.append(nv)

        df= df.drop(no_n, axis=1)
        
        self.df=df

        return self.df
    
    
    
    def update_metadata(self,column_name=[]):
        
        # update meta data
        
        return True
   
