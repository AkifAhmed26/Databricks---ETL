# Databricks notebook source
# MAGIC %run /Workspace/Users/akifahmed1999@gmail.com/Project2

# COMMAND ----------

df_movies = df_final.filter(df_final["type"] == "Movie")
df_shows = df_final.filter(df_final["type"] == "TV Show")


# COMMAND ----------

display(df_movies)


# COMMAND ----------

display(df_shows)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import count, col

# Define a window partitioned by country
country_window = Window.partitionBy("country")

# Add a new column with total count of titles per country
df_with_country_count = df_movies.withColumn("titles_in_country", count("*").over(country_window))

display(df_with_country_count)


# COMMAND ----------

from pyspark.sql.functions import split, explode, trim, col

# Split country column by comma and explode
df_exploded = df_movies.withColumn("country_split", split(col("country"), ",")) \
                .withColumn("country_single", explode("country_split")) \
                .withColumn("country_single", trim(col("country_single")))  # remove extra spaces

# Now group by the single country and count
df_country_counts = df_exploded.groupBy("country_single") \
                               .count() \
                               .orderBy("count", ascending=False)

display(df_country_counts)


# COMMAND ----------

