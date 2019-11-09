# mltrons-auto-data-prep :Tool kit that automate the Data Preparation

## What is it?

**Mltrons-auto-data-prep** is a Python package providing flexible and automated way of 
data preparation in any size of the raw data.It uses **Machine Learning** and **Deep Leaning**
techniques with the *pyspark* back-end architecture to clean and prepare TBs of data on clusters at scale.


## Main Features
Here are just a few of the things that **Mltrons-auto-data-prep** does well:

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
- [NumPy](https://www.numpy.org): 
- [pandas](https://pandas.pydata.org)
- [python-dateutil](https://labix.org/python-dateutil): 
- [pytz](https://pythonhosted.org/pytz):
- see full list of dependicies [here](https://github.com/ms8909/mltrons-auto-data-prep/blob/master/requirements.txt)