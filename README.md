# mltrons dptron: Dirty Data in, Clean Data Out!
https://pypi.org/project/mltronsAutoDataPrep/


## Introduction

Data is the most important element for data analysis. Real world data is unclean with a lot of spelling errors, missing values, formatting issues, skewness, no encoding or aggregation which makes it the most time-consuming & cumbersome task for analysts & scientists. As most of the scientists spend time around 80% of their time cleaning & preparing data, therefore we’re introducing dptron to make that process extremely easier and faster!

Dptron is an in-memory platform built for distributed & scalable data cleaning & preparation. DPtron is written in Python and is built on PySpark to deal with large amounts of data seamlessly. It uses an implementation of machine learning and deep learning algorithms to perform important data cleaning & preparation steps automatically. Dptron is extensible so that developers, analysts & scientists can streamline the process of data cleaning & preparation for better decision making while becoming more productive. 

Decision making is better & easier if the data is clean otherwise it’s garbage-in and garbage-out. 


## Important Features

- Supports connection with AWS S3
- Supports upto 10TB of data size
- Treats spelling mistakes and other inconsistencies in URLs
- Detects & treats skewness in data
- Feature engineering for time variable
- Treats & fills NULL values by using deep learning (next iteration)
- Treats spelling mistakes and other inconsistencies in other variables (next iteration)


## Get started with dptron!

Installation
### Installing On Mac Os
Open up your terminal and install Java8 required for pySpark:
```sh
brew cask install adoptopenjdk/openjdk/adoptopenjdk8**
```
After installing Java8, set it as your default Java version:
```sh
/usr/libexec/java_home -V**
```
This will output thefollowing:

Matching Java Virtual Machines (3):
```
1.8.0_05, x86_64:   "Java SE 8" /Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home
1.6.0_65-b14-462, x86_64:   "Java SE 6" /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home
1.6.0_65-b14-462, i386: "Java SE 6" /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home
/Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home
```

Pick the version you want to be the default (i.e 1.6.0_65-b14-462) then:
```sh
export JAVA_HOME=/usr/libexec/java_home -v 1.8**
```

After you've successfully install Java8, install dptron with the following command: 
```sh
pip install mltronsAutoDataPrep
```

### Installing on Windows

It's important that you replace all the paths that include the folder "Program Files" or "Program Files (x86)" to avoid future problems while running Spark.

If you have Java already installed, you still need to fix the JAVA_HOME and PATH variables:

**Step 1. Replace "Program Files" with "Progra~1"**

**Step 2. Replace "Program Files (x86)" with "Progra~2"**
```
Example: "C:\Program FIles\Java\jdk1.8.0_161" --> "C:\Progra~1\Java\jdk1.8.0_161"
```
Before you start make sure you have Java 8 installed and the environment variables correctly defined1:

**Step 3. Download Java JDK 8 from [Java's official website] (https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)**

Set the following environment variables:

**Step 4. JAVA_HOME = C:\Progra~1\Java\jdk1.8.0_161**

**Step 5. PATH += C:\Progra~1\Java\jdk1.8.0_161\bin**

After you've successfully installed and configured Java8, install dptron with the following command: 
```sh
pip install mltronsAutoDataPrep
```

## How to install



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
