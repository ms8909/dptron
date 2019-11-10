# mltrons-auto-data-prep :Tool kit that automate Data Preparation

## What is it?

**Mltrons-auto-data-prep** is a Python package providing flexible and automated way of 
data preparation in any size of the raw data.It uses **Machine Learning** and **Deep Leaning**
techniques with the **pyspark** back-end architecture to clean and prepare TBs of data on clusters at scale.


## Main Features
Here are just a few of the things that **Mltrons-auto-data-prep** does well:

- Data Can be read from multiple Sources such as **S3 bucket** or **Local PC**

- Handle Any size of data even in Tbs using **Py-spark**

- Filter out **Features** with Null values more than the threshold

- Filter out **Features** with same value for all rows

- Automatically detects the data type of features

- Automatically detects datetime features and split in multiple usefull features

- Automatically detects features containing **URLs** and remove duplications

- Automatically detects **Skewed** features and minimize skewness



## Where to get it
The source code is currently hosted on **GitHub** at:
https://github.com/ms8909/mltrons-auto-data-prep

The **pypi** project is at :
https://pypi.org/project/mltronsAutoDataPrep/


## How to install

```sh
pip install mltronsAutoDataPrep
```

## Dependencies
- [PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
- [NumPy](https://www.numpy.org)
- [pandas](https://pandas.pydata.org)
- [python-dateutil](https://labix.org/python-dateutil) 
- [pytz](https://pythonhosted.org/pytz)
- see full list of dependicies [here](https://github.com/ms8909/mltrons-auto-data-prep/blob/master/requirements.txt)

## How to use 


### 1. Reading data functions

- **address** to give the path of the file

- **local** to give the file exist on local pc or s3 bucket

- **file_format** to give the format of the file (csv,excel,parquet)

- **s3** s3 bucket credentials if data on s3 bucket


```python
from mltronsAutoDataPrep.lib.v2.Operations.readfile import ReadFile as rf

res = rf.read(address="test.csv", local="yes", file_format="csv", s3={})
```



### 2. Drop Features containing Null of certain threshold

- provide dataframe with threshold of null values 

- return the list of columns containing null values more then the threshold

```python
from lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol

res = read.read("test.csv", file_format='csv')

drop_col = DropNullValueCol()
columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
df = res.drop(*columns_to_drop)
```


### 3. Drop Features containing same values 

- provide dataframe 

- return the list of columns containing same values

```python
from lib.v2.Middlewares.drop_col_with_same_val import DropSameValueColumn


drop_same_val_col = DropSameValueColumn()
columns_to_drop = drop_same_val_col.delete_same_val_com(res)
df = res.drop(*columns_to_drop)
```

### 4. Cleaned Url Features

- Automatically detects features containing Urls

- Pipeline structure to clean the urls using **NLP** techniques

```python

from lib.v2.Pipelines.etl_pipeline import EtlPipeline

etl_pipeline = EtlPipeline()
etl_pipeline.custom_url_transformer(res)
res = etl_pipeline.transform(res)

```


### 5. Split Date Time features

- Automatically detects features containing date/time

- Split date time into usefull multiple feautures (day,month,year etc)


```python
from lib.v2.Pipelines.etl_pipeline import EtlPipeline


etl_pipeline = EtlPipeline()
etl_pipeline.custom_date_transformer(res)
res = etl_pipeline.transform(res)

```


### 6 . Filling Missing Values

- Using Deep Learning techniques Missing values are filled


```python

etl_pipeline = EtlPipeline()
etl_pipeline.custom_filling_missing_val(res)
res = etl_pipeline.transform(res)

```


