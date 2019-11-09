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

