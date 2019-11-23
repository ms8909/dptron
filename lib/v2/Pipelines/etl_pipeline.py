from ..imports import *

# Including Transformers

from ..Transformers.url_transformer import *
from ..Transformers.date_transformer import *
from ..Transformers.drop_transformer import *
from ..Transformers.skewness_transformer import *
from ..Transformers.type_to_double_transformer import *
from ..Transformers.change_columns_order import *
from ..Transformers.convert_nan_to_null import *
# Including Middlewares

from ..Middlewares.dtype_conversion import *
from ..Middlewares.fetch_url_columns import *
from ..Middlewares.fetch_skewed_columns import *
from ..Middlewares.drop_col_with_null_val import *
from ..Middlewares.drop_col_with_same_val import *
from ..Middlewares.fetch_datetime_columns import *


class EtlPipeline():

    def __init__(self, ID="", key="", local=True, s3=False):
        # variables
        self.pipeline = None
        self.stages = []
        # selected_columns specifies the order of the columns
        self.param = {"local": local, "s3": s3, "dropped_variables": [], "selected_variables": [],
                      "all_variables": [], "existed_variables": [],
                      "numerical_variables": [], "categorical_variables": []}

        done = True
        while done:
            self.sc = pyspark.SparkContext.getOrCreate()
            if self.sc is None:
                time.sleep(2)
            else:
                done = False

        # configuration
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if local is False:
            self.hadoop_conf.set("fs.s3n.awsAccessKeyId", ID)
            self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", key)
        self.sql = pyspark.sql.SparkSession(self.sc)

        # methods

    def set_parameter(self, key, value):
        self.param[key] = value

    def get_parameter(self, key=None):
        if key is None:
            return self.param
        return self.param[key]

    # methods
    def save_pipeline(self, bucket='mltrons', path=None):
        if path is None:
            # generate own path
            path = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))

        if self.param["s3"] is True:
            self.pipeline.save('s3n://' + bucket + '/' + path)

        if self.param["local"] is True:
            self.pipeline.save(os.path.join('spark', path))

        return path

    def load_pipeline(self, df=None, bucket='mltrons', path=None):
        if df is None:
            logger.warn("PLease provide test df")
            return False

        if self.param["s3"] is True:
            pipeline = Pipeline.load('s3n://' + bucket + '/' + path)

        if self.param["local"] is True:
            pipeline = Pipeline.load(os.path.join('spark', path))

        self.pipeline = pipeline.fit(df)
        return self.pipeline

    @staticmethod
    def remove_y_from_variables(variables, y_var, training=True):
        new_variable = []
        if training is True:
            for v in variables:
                if y_var == v[0]:
                    pass
                else:
                    new_variable.append(v)
        return new_variable

    @staticmethod
    def convert_y_to_float(df, y_var):
        return df.withColumn(y_var, funct.col(y_var).cast(DoubleType()))

    def transform(self, df=None):
        if df is None:
            logger.warn("Please provide dataframe to build")
            return False

        if self.pipeline is None:
            logger.warn("Please build or load the pipeline first.")
            return False
        df = self.pipeline.transform(df)
        return df

    def add_new_param(self, drp_var_array, params):
        current_array = self.param[params]
        if type(drp_var_array) == type("str"):
            drp_var_array = [drp_var_array]

        for i in drp_var_array:
            current_array.append(i)
        self.param[params] = current_array

    def remove_new_param(self, drop_var_array, params):
        """

        :param drop_var_array:
        :param params:
        :return:
        """
        if type(drop_var_array) == type("str"):
            drop_var_array = [drop_var_array]

        for i in drop_var_array:
            self.param[params].remove(i)

    def find_params(self, df):
        self.param["all_variables"] = df.columns
        self.param["selected_variables"] = df.columns
        self.param['existed_variables'] = df.columns

    def time_new_variables(self, variable_list, keyword_list):
        """

        :param variable_list:
        :param keywors_list:
        :return:
        """
        column_name = []
        for i in variable_list:
            for k in keyword_list:
                column_name.append(i + k)

        return column_name

    def build_pipeline(self, df=None):

        if df is None:
            logger.error("Please provide dataframe to build a pipeline")
            return False

        """Update param """
        self.find_params(df)

        """1. Find variables with 70% or more null values"""
        try:
            variables = self.variables_with_null_more_than(df, percentage=60)
        except Exception as e:
            logger.error(e)
            logger.error("in finding columns with a lot of missing values. 1")
            return False
        # Drop all these variables.
        try:
            self.drop_these_variables(variables)
            self.add_new_param(variables, "dropped_variables")
            self.remove_new_param(variables, "existed_variables")
            self.remove_new_param(variables, "selected_variables")

        except Exception as e:
            logger.error(e)
            logger.error("in dropping variables. 1")
            return False

        logger.warn("1. Find variables with 70% or more null values")

        """ 2. Find which variable contains time and what the format of time is"""
        try:
            time_variables = self.find_all_time_variables(df)
            keyword_list = ['_year', '_month', '_day', '_dayofweek', '_hour', '_minutes', '_seconds']
            new_selected_col = self.time_new_variables(time_variables, keyword_list)
            logger.warn("time variables")
            logger.warn(time_variables)
        except Exception as e:
            logger.error(e)
            return False
        # handle time
        try:
            self.split_change_time(time_variables)

            self.add_new_param(new_selected_col, "selected_variables")
            self.add_new_param(time_variables, "dropped_variables")
            self.remove_new_param(time_variables, "existed_variables")
            self.remove_new_param(time_variables, "selected_variables")

        except Exception as e:
            logger.error(e)
            logger.error("in split time. 2")
            return False

        logger.warn("2. Find which varaible contains time and what the format of time is")

        """3. Find all variables with single value"""
        try:
            same_variables = self.variables_with_same_val(df)
        except Exception as e:
            logger.error(e)
            logger.error("in finding columns with only one value. 3")
            return False
        # Drop all these variables.
        try:
            self.drop_these_variables(same_variables)

            self.add_new_param(same_variables, "dropped_variables")
            self.remove_new_param(same_variables, "existed_variables")
            self.remove_new_param(same_variables, "selected_variables")

        except Exception as e:
            logger.error(e)
            logger.error("in dropping variables. 3")
            return False

        logger.warn("3. Find all variables with single value")

        """5. Treat duplications"""
        try:
            url_variables = self.find_variables_containing_urls(df)
            logger.warn("done")
        except Exception as e:
            logger.error(e)
            logger.error("in finding variables containing urls. 5")
            return False
        try:
            var = self.clean_variable_containing_urls(url_variables)
        except Exception as e:
            logger.error(e)
            logger.error("in fixing variables containing urls. 5")
            return False

        # """6. convert variable type """
        # try:
        #     int_variables = self.correct_variable_types(df)
        #
        # except Exception as e:
        #     logger.error(e)
        #     logger.error("in finding int variables saved as strings. 4")
        #     return False
        # # Change type
        # try:
        #     self.int_to_double(df.dtypes, int_variables)
        #     self.add_new_param(int_variables, "int_variables")
        #
        # except Exception as e:
        #     logger.error(e)
        #     logger.error("int to double. 4")
        #     return False
        #
        # logger.warn("4. convert variable type")

        """6. Treat missing values in numeric variables."""
        try:
            numeric_variables = self.find_variables_types(df.dtypes)
            self.int_to_double(df.dtypes, numeric_variables)


        except Exception as e:
            logger.error(e)
            logger.error("in finding all numeric variables. 6")
            return False
        try:
            self.handle_missing_values(numeric_variables)
        except Exception as e:
            logger.error(e)
            logger.error("in filling numeric variables with mean.")
            return False

        """7. Minimizing Skewness."""
        try:
            variables = self.fetch_skewed_features(df)

            self.skewed_transformer(variables)
        except Exception as e:
            logger.error(e)
            logger.error("in minimizing skewness.")
            return False

        logger.warn("6. Treat missing values in numeric variables")

        """8. Encode categorical variables"""
        try:
            self.encode_categorical_var()
        except Exception as e:
            logger.error(e)
            logger.error("in encoding categorical variables.")
            return False

        """9.Impute missing values in categorical variables."""
        try:
            self.handle_missing_values(self.param["categorical_variables"])
        except Exception as e:
            logger.error(e)
            logger.error("in imputing categorical variables.")
            return False

        """10.Changing variables order"""
        logger.warn("Changing Order values")
        try:
            self.change_order_variables()
        except Exception as  e:
            logger.error(e)

        try:
            """Initialize spark pipeline."""
            pi = Pipeline(stages=self.stages)

            self.pipeline = pi.fit(df)
            return self.pipeline
        except Exception as e:
            logger.error(e)
            logger.error("after calling all stages into pipeline")

    def fetch_skewed_features(self, df):
        """

        :param df:
        :return:
        """
        n = FetchSkewedCol()
        features = n.skewed_features(df, existed_variables=self.param["existed_variables"])
        return features

    def find_variables_containing_urls(self, df):

        """Find all variables containing urls"""
        n = FetchUrlCol()
        variables = n.fetch_columns_containing_url(df, existed_variables=self.param['existed_variables'])
        return variables

    def clean_variable_containing_urls(self, df, variables=[]):

        """Clean all the variables containing urls"""
        for v in variables:
            d = UrlTransformer(column=v)
            self.stages += [d]

        return True

    def change_order_variables(self):

        n = ChangeColumnsOrder(column=self.param['selected_variables'])
        self.stages += [n]

    def variables_with_null_more_than(self, df, percentage=20):

        """Find all the variables that contain null values more than 20%"""
        n = DropNullValueCol()
        variables = n.delete_var_with_null_more_than(df, threshold=percentage)
        return variables

    def correct_variable_types(self, df):

        """Find all numeric variables saved as string."""
        n = DtypeConversion()

        # variables

        variables = n.find_numeric_variables_saved_as_string(df, selected_variables=self.param["selected_variables"])
        return variables

    def find_all_time_variables(self, df):

        """Find all variables that contain time"""
        n = FetchDateTimeCol()
        variables = n.find_datetime_features(df, existed_variables=self.param["existed_variables"])
        return variables

    def variables_with_same_val(self, df):

        """find variables that contain save value."""
        n = DropSameValueColumn()
        variables = n.delete_same_val_com(df, existed_variables=self.param["existed_variables"])
        return variables

    def drop_these_variables(self, variables):
        for v in variables:
            d = DropTransformer(column=v)
            self.stages += [d]

    def int_to_double(self, dtypes, int_variables):
        co = ['bigint', 'int']

        for column in dtypes:
            if column[0] not in int_variables:
                continue
            elif column[1] in co:
                # time to transform
                change = TypeDoubleTransformer(column=column[0])
                self.stages += [change]

    def find_variables_types(self, dtypes):
        co = ['bigint', 'int', 'double', 'float']

        numeric_variables = []
        categorical_variables = []
        for column in dtypes:
            if column[0] not in self.param['existed_variables']:
                continue
            elif column[1] in co:
                numeric_variables.append(column[0])
            else:
                categorical_variables.append(column[0])

        self.add_new_param(categorical_variables, 'categorical_variables')
        self.add_new_param(numeric_variables, 'numerical_variables')

        return numeric_variables

    def encode_categorical_var(self):
        encode_variables_new = []

        for column in self.param["categorical_variables"]:
            stringIndexer = StringIndexer(inputCol=column, outputCol=column + '_index').setHandleInvalid("keep")
            self.stages += [stringIndexer]
            d = DropTransformer(column)
            self.add_new_param([column], "dropped_variables")
            # self.remove_new_param(column, "existed_variables")
            encode_variables_new.append(column + '_index')
            self.remove_new_param([column], "selected_variables")
            self.add_new_param([column + '_index'], "selected_variables")

            # self.remove_new_param(column, 'existed_variables')

            self.stages += [d]

        self.param['categorical_variables'] = encode_variables_new

    def handle_missing_values(self, variables):

        imputer = Imputer(
            inputCols=variables,
            outputCols=variables
        )
        self.stages += [imputer]

    def split_change_time(self, time_variables):
        for v in time_variables:
            time_data = DateTransformer(column=v)
            self.stages += [time_data]

            # To Do

    def skewed_transformer(self, skewed_columns):
        for col in skewed_columns:
            skewed_data = SkewnessTransformer(column=col)
            self.stages += [skewed_data]

    def custom_skewness_transformer(self, df):
        """

        :param df:
        :return:
        """
        try:
            n = FetchSkewedCol()
            features = n.skewed_features(df, existed_variables=df.columns)
            self.skewed_transformer(features)
            model = Pipeline(stages=self.stages)
            self.pipeline = model.fit(df)


        except Exception as e:
            print(e)

    def convert_nans_into_null(self, df):
        """
        Replace Nans ,None none,N/A,NA with
        :param df:
        :return:
        """
        model = NanToNullConvertor(list_of_col=df.columns)
        self.stages += [model]

    def fetch_numerical_columns(self, df):

        """Find all variables containing urls"""
        n = DtypeConversion()
        variables = n.find_numerical_features(df, existed_features=df.columns)
        return variables

    def convert_str_to_double(self, df, numerical_col):
        """

        :param df:
        :param numerical_col:
        :return:
        """
        model = TypeDoubleTransformer(list_of_col = numerical_col)
        self.stages += [model]

    def custom_filling_missing_val(self, df):
        try:
            self.param['existed_variables'] = df.columns
            self.convert_nans_into_null(df)
            # numeric_variables = self.find_variables_types(df.dtypes)
            numeric_variables = self.fetch_numerical_columns(df)

            self.convert_str_to_double(df,numeric_variables)
            # self.int_to_double(df.dtypes, numeric_variables)

            self.handle_missing_values(numeric_variables)
            model = Pipeline(stages=self.stages)
            self.pipeline = model.fit(df)

        except Exception as e:
            logger.error(e)

    def custom_date_transformer(self, df):
        """

        :param df:
        :return:
        """
        time_variables = self.find_all_time_variables(df)
        stage = self.custom_datetime_pipeline(time_variables)
        print(stage)
        pi = Pipeline(stages=stage)
        self.pipeline = pi.fit(df)

    def custom_datetime_pipeline(self, time_variables):
        stages = []
        for v in time_variables:
            time_data = DateTransformer(column=v)
            stages += [time_data]
        return stages

    def custom_url_transformer(self, df):
        try:
            url_variables = self.find_variables_containing_urls(df)
            logger.warn("done")
        except Exception as e:
            logger.error(e)
            logger.error("in finding variables containing urls. 5")
            return False
        try:
            var = self.custom_urls_pipeline(url_variables)
            pi = Pipeline(stages=var)

            self.pipeline = pi.fit(df)
        except Exception as e:
            logger.error(e)
            logger.error("in fixing variables containing urls. 5")
            return False

    def custom_urls_pipeline(self, variables=[]):

        """Clean all the variables containing urls"""
        stages = []
        for v in variables:
            d = UrlTransformer(column=v)
            stages += [d]

        return stages


""" 
Pipeline 1 remaining steps


        # 8. Find which variables are important.
        # Drop unimportant variables.

"""

"""
Pipeline 2

        # Imputations of categorical variable using datawig.

"""

"""
Pipeline 3

        # 9. Encode categorical variables
        try:

            self.encode_categorical_var()
        except Exception as e:
            print(e, "categorical to float")
            return False



        # 10. Find the order of all variables.
        # Rearrange dataframe using above order.

"""
