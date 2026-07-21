# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS akifsprojects.project2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS akifsprojects.project2.amazon;
# MAGIC

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .option("escape","\"")\
    .load('/Volumes/akifsprojects/project2/amazon/amazon_prime_titles.csv')

# COMMAND ----------

# Writing the file to our bronze layer, changing into parquet format.
df.write.format("parquet")\
    .mode("overwrite")\
    .save("/Volumes/akifsprojects/project2/amazon/bronze/")


# COMMAND ----------

display(df)

# COMMAND ----------

df_parquet = spark.read.format("parquet") \
    .load("/Volumes/akifsprojects/project2/amazon/bronze/")

display(df_parquet)


# COMMAND ----------

df_nonulls = df.filter(df.show_id.isNotNull())
display(df_nonulls)


# COMMAND ----------

df_noempties = df_nonulls.filter(df.show_id != "")
display(df_noempties)

# COMMAND ----------

#dropping duplicates
df_no_dup = df_noempties.dropDuplicates()

display(df_no_dup)


# COMMAND ----------

# removing non-sensical data from the show_id
from pyspark.sql.functions import col, trim, lower

df_valid_ids = df_no_dup.filter(
    lower(trim(col("show_id"))).rlike(r"^s\d+$")
)

display(df_valid_ids)



# COMMAND ----------


# removing non-sensical data from type rows. For that, we have to first find if we have any data in the type column that is not either Movie or TV Show.
from pyspark.sql.functions import when

df_labeled = df_valid_ids.withColumn(
    "is_valid_type",
    when(col("type").isin("Movie", "TV Show"), True).otherwise(False)
)

display(df_labeled)




# COMMAND ----------

# Filter rows where is_valid_type is False
df_invalid = df_labeled.filter(col("is_valid_type") == False)

display(df_invalid)


# COMMAND ----------

display(df_valid_ids)

# COMMAND ----------

df_valid_titles = df_valid_ids.fillna({"title" : "Unknown"})
display(df_valid_titles)

# COMMAND ----------

#replacing nulls in director, cast, and country columns
df_valid_dcc = df_valid_titles.fillna({"director" : "Unknown","cast" : "Unknown","country" : "Unknown", "description" : "Unknown", "rating" : "Unknown", "listed_in" : "Unknown"})
display(df_valid_dcc)


# COMMAND ----------

df_replace_dates = df_valid_dcc.fillna({"date_added" : "January 1, 1900", "release_year" : 1900})
display(df_replace_dates)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, regexp_extract, expr

df_casted = (
    df_replace_dates
    # convert date_added string → date (ISO format: yyyy-MM-dd)
    .withColumn("date_added", to_date(col("date_added"), 'MMMM d, yyyy'))
    # cast release_year to int
    .withColumn("release_year", col("release_year").cast("int"))\
    # extract numbers from duration before casting
    .withColumn("duration", regexp_extract(col("duration"), r"(\d+)", 1).cast("int"))
    )


# COMMAND ----------

display(df_casted)

# COMMAND ----------

df_casted.select("rating").distinct().show()


# COMMAND ----------

from pyspark.sql.functions import col, when

df_classified = df_casted.withColumn(
    "audience_category",
    when(col("rating").isin("18+", "16+", "R", "NC-17", "TV-MA", "UNRATED", "NR", "TV-NR"), "Adult")
    .otherwise("AllOthers")
)

df_classified.select("rating", "audience_category").distinct().show()


# COMMAND ----------

display(df_classified)

# COMMAND ----------

# Writing the cleaned file to our silver layer, changing into delta format.
df_classified.write.format("delta")\
    .mode("overwrite")\
    .save("/Volumes/akifsprojects/project2/amazon/silver/")

# COMMAND ----------

df_delta = spark.read.format("delta").load("/Volumes/akifsprojects/project2/amazon/silver/")
display(df_delta)


# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 1 Transformations:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS akifsprojects.project2.gold_table
# MAGIC (
# MAGIC   show_id STRING,
# MAGIC   type STRING,
# MAGIC   title STRING,
# MAGIC   director STRING,
# MAGIC   cast STRING,
# MAGIC   country STRING,
# MAGIC   date_added DATE,
# MAGIC   release_year INT,
# MAGIC   rating STRING,
# MAGIC   duration INT,
# MAGIC   listed_in STRING,
# MAGIC   description STRING,
# MAGIC   audience_category STRING,
# MAGIC   hashkey STRING,
# MAGIC   updatedby STRING,
# MAGIC   updateddate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted akifsprojects.project2.gold_table 

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp
from pyspark.sql.functions import sha2, concat_ws, col



# Adding a literal column to my source table
df_final = df_delta.withColumn("updatedby", lit("akif"))

# Adding a timestamp column to my source table
df_final = df_final.withColumn("updateddate", to_timestamp(lit("1900-01-01 00:00:00")))

# Convert column names to Column objects
cols = [col(c) for c in df_final.columns]

# Adding a hashkey column based on concatenating other columns
from pyspark.sql.functions import sha2, concat_ws
df_final = df_final.withColumn("hashkey", sha2(concat_ws("||", *cols), 256))


# COMMAND ----------

display(df_final)

# COMMAND ----------

from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "akifsprojects.project2.gold_table")

target_table.toDF().show()


# COMMAND ----------

df_compare = df_final.alias("source").join(target_table.toDF().alias("target"),(col("source.show_id")==col("target.show_id")) & (col("source.hashkey")==col("target.hashkey")),"anti").select("source.*")
display(df_compare)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

target_table.alias("tgt").merge(df_compare.alias("src"),"tgt.show_id = src.show_id")\
    .whenMatchedUpdate(set={
        "tgt.show_id" : "src.show_id",
        "tgt.type" : "src.type",
        "tgt.title" : "src.title",
        "tgt.director" : "src.director",
        "tgt.cast" : "src.cast",
        "tgt.country" : "src.country",
        "tgt.date_added" : "src.date_added" ,
        "tgt.release_year" : "src.release_year",
        "tgt.rating" : "src.rating",
        "tgt.duration" : "src.duration",
        "tgt.listed_in" : "src.listed_in",
        "tgt.description" : "src.description",
        "tgt.audience_category" : "src.audience_category",
        "tgt.updatedby" : lit("databricks-upd"),
        "tgt.updateddate" :current_timestamp(),
        "tgt.hashkey" : "src.hashkey"
})\
    .whenNotMatchedInsertAll()\
        .execute()

# COMMAND ----------

from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "akifsprojects.project2.gold_table")

target_table.toDF().show()
display(target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM akifsprojects.project2.gold_table