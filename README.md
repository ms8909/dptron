# mltrons-auto-data-prep :Tool-kit that automates Data Preparation

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

## How to install Java 8 (only supported by pyspark)
### In Mac Os
In your terminal, write:

**1. brew cask install adoptopenjdk/openjdk/adoptopenjdk8**

Now, you need to set java8 as your default version. To do this:
First run 

**2. /usr/libexec/java_home -V**

which will output something like the following:
Matching Java Virtual Machines (3):
```
1.8.0_05, x86_64:   "Java SE 8" /Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home
1.6.0_65-b14-462, x86_64:   "Java SE 6" /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home
1.6.0_65-b14-462, i386: "Java SE 6" /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home
/Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home
```

Pick the version you want to be the default (i.e 1.6.0_65-b14-462) then:

**3. export JAVA_HOME=/usr/libexec/java_home -v 1.8**

### In Windows Os

It’s important that you replace all the paths that include the folder “Program Files” or “Program Files (x86)” as explained below to avoid future problems when running Spark.
If you have Java already installed, you still need to fix the JAVA_HOME and PATH variables

**1. Replace “Program Files” with “Progra~1”**

**2. Replace “Program Files (x86)” with “Progra~2”**

```
Example: “C:\Program FIles\Java\jdk1.8.0_161” --> “C:\Progra~1\Java\jdk1.8.0_161”
```
Before you start make sure you have Java 8 installed and the environment variables correctly defined1:

**3. Download Java JDK 8 from [Java’s official website](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)**

Set the following environment variables:

**4. JAVA_HOME = C:\Progra~1\Java\jdk1.8.0_161**

**5. PATH += C:\Progra~1\Java\jdk1.8.0_161\bin**


## How to install

```sh
pip install mltronsAutoDataPrep
```

## Dependencies
- [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
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
from mltronsAutoDataPrep.lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol

res = rf.read("test.csv", file_format='csv')

drop_col = DropNullValueCol()
columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
df = res.drop(*columns_to_drop)
```


### 3. Drop Features containing same values 

- provide dataframe 

- return the list of columns containing same values

```python
from mltronsAutoDataPrep.lib.v2.Middlewares.drop_col_with_same_val import DropSameValueColumn


drop_same_val_col = DropSameValueColumn()
columns_to_drop = drop_same_val_col.delete_same_val_com(res)
df = res.drop(*columns_to_drop)
```

### 4. Cleaned Url Features

- Automatically detects features containing Urls

- Pipeline structure to clean the urls using **NLP** techniques

```python

from mltronsAutoDataPrep.lib.v2.Pipelines.etl_pipeline import EtlPipeline

etl_pipeline = EtlPipeline()
etl_pipeline.custom_url_transformer(res)
res = etl_pipeline.transform(res)

```


### 5. Split Date Time features

- Automatically detects features containing date/time

- Split date time into usefull multiple feautures (day,month,year etc)


```python
from mltronsAutoDataPrep.lib.v2.Pipelines.etl_pipeline import EtlPipeline


etl_pipeline = EtlPipeline()
etl_pipeline.custom_date_transformer(res)
res = etl_pipeline.transform(res)

```


### 6. Filling Missing Values 

- Using Deep Learning techniques Missing values are filled


```python
from mltronsAutoDataPrep.lib.v2.Pipelines.etl_pipeline import EtlPipeline


etl_pipeline = EtlPipeline()
etl_pipeline.custom_filling_missing_val(res)
res = etl_pipeline.transform(res)

```


### 7. Removing Skewness from features


- Automatically detects which column contains skewness

- Minimize skewness using statistical methods

```python
from mltronsAutoDataPrep.lib.v2.Pipelines.etl_pipeline import EtlPipeline


etl_pipeline = EtlPipeline()
etl_pipeline.custom_skewness_transformer(res)
res = etl_pipeline.transform(res)
```
