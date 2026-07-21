# Databricks---ETL
Transformations and Analysis of TV Shows and Movies on Amazon Prime.

In this project, we are fetching a dataset from the Kaggle website which has a
dataset on Amazon Prime’s TV Shows and Movies.
Link to the dataset: https://www.kaggle.com/datasets/shivamb/amazon-primemovies-
and-tv-shows
The objective of this project is to clean the datatset and perform SCD Type-1
transformation on them.
We are following a medallion architecture here, where-in, the raw data will be
stored in the bronze layer in volumes in the parquet format, then the data will be
cleaned and stored in the silver layer in delta format, again in volumes and finally,
SCD-1 transformations will be performed and business-ready data will be stored
in a managed table in the gold layer. We are also doing the analysis of our tables
after breaking them into two (Movies, TV Shows) in a separate databricks
notebook.
