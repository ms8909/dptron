from ..imports import *


class DtypeConversion(object):

    def __init__(self):
        pass

    def run(self, df):
        try:
            numeric_columns = self.find_numeric_variables_saved_as_string(df)
            df = self.update_variable_types(df, numeric_columns)
            return df

        except Exception as e:
            logger.error(e)

    def find_numeric_variables_saved_as_string(self, df, dropped_variables=[]):
        """numeric variables"""
        count = int(df.count())
        print("Total number of rows in the data are: ", count)
        if count > 500:
            count = 500

        p = df.limit(count).toPandas()

        numeric_columns = []
        columns = self.string_variables(df, dropped_variables)  # only take string variables
        for c in columns:
            counter = 0
            error = []
            float_values = []
            for i in range(count):
                try:
                    v = float(p[c][i])
                    counter = counter + 1
                    float_values.append(v)
                except Exception as e:
                    logger.error(e)
                    error.append(str(p[c][i]))

            if len(error) == 0:
                numeric_columns.append(c)
            elif len(set(float_values)) < 5:
                if len(set(error)) <= 1 and len(set(float_values)) != 0:
                    numeric_columns.append(c)
            elif len(float_values) / count * 100 > 95:
                numeric_columns.append(c)

        return numeric_columns

    @staticmethod
    def update_variable_types(df, numeric_columns):
        for c in numeric_columns:
            df = df.withColumn(c, funct.col(c).cast(DoubleType()))

        return df

    @staticmethod
    def string_variables(df, dropped_variables):
        s_variables = []
        for c in df.dtypes:
            if c[0] in dropped_variables:
                pass
            elif c[1] == "string":
                s_variables.append(c[0])

        return s_variables

    def find_numerical_features(self, df, existed_features=[]):
        numeric_columns = []
        total_size = df.count()
        for i in existed_features:
            try:
                df = df.withColumn(i, funct.regexp_replace(funct.col(i), "[^a-zA-Z0-9.]", ""))
                val = df.withColumn(i, df[i].cast(DoubleType()))
                total_valid_values = val.dropna().count()
                if total_valid_values > 0:
                    pct_float = (total_valid_values / total_size) * 100
                    if pct_float > 95:
                        numeric_columns.append(i)

            except Exception as e:
                print("exception")
                print(e)

        logger.warn("numerical features:")
        logger.warn(numeric_columns)

        return numeric_columns
