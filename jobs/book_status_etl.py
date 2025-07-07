import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, when, current_timestamp, concat_ws, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("book_status_etl_job", args={})

# Read source table
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_project",
    table_name="books"
)
raw_df = raw_dyf.toDF().withColumn("ingested_at", current_timestamp())
print("test")
# Read existing status table
try:
    existing_dyf = glueContext.create_dynamic_frame.from_catalog(
        database="ecommerce_clean_db",
        table_name="book_status"
    )
    existing_df = existing_dyf.toDF()
    if len(existing_df.columns) == 0:
        raise Exception("Empty schema fallback")
except:
    schema = StructType([
        StructField("book_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("author", StringType(), True),
        StructField("price", StringType(), True),
        StructField("stock", IntegerType(), True),
        StructField("flag", BooleanType(), True),
        StructField("message", StringType(), True),
        StructField("last_updated", TimestampType(), True)
    ])
    existing_df = spark.createDataFrame([], schema)

# Rename columns before join
new_df = raw_df.selectExpr(
    "book_id as new_book_id",
    "title as new_title",
    "genre as new_genre",
    "author as new_author",
    "price as new_price",
    "stock as new_stock",
    "ingested_at as new_ingested_at"
)

old_df = existing_df.selectExpr(
    "book_id as old_book_id",
    "title as old_title",
    "genre as old_genre",
    "author as old_author",
    "price as old_price",
    "stock as old_stock",
    "flag",
    "message",
    "last_updated"
)

# Outer join on book_id
joined_df = new_df.join(
    old_df,
    new_df.new_book_id == old_df.old_book_id,
    how="outer"
)

# Compare and mark changes
comparison_df = joined_df.withColumn("book_id", coalesce("new_book_id", "old_book_id")) \
    .withColumn("is_new", col("old_book_id").isNull()) \
    .withColumn("is_updated",
        (col("new_title") != col("old_title")) |
        (col("new_genre") != col("old_genre")) |
        (col("new_author") != col("old_author")) |
        (col("new_price") != col("old_price")) |
        (col("new_stock") != col("old_stock"))
    ) \
    .withColumn("message",
        when(col("is_new"), lit("added product"))
        .when(col("is_updated"), concat_ws(", ",
            when(col("new_title") != col("old_title"), lit("title")),
            when(col("new_genre") != col("old_genre"), lit("genre")),
            when(col("new_author") != col("old_author"), lit("author")),
            when(col("new_price") != col("old_price"), lit("price")),
            when(col("new_stock") != col("old_stock"), lit("stock"))
        )).otherwise(lit(""))
    ) \
    .withColumn("flag", col("is_new") | col("is_updated")) \
    .withColumn("last_updated", when(col("flag"), current_timestamp()).otherwise(col("last_updated")))

# Final cleaned output
final_df = comparison_df.select(
    "book_id",
    coalesce("new_title", "old_title").alias("title"),
    coalesce("new_genre", "old_genre").alias("genre"),
    coalesce("new_author", "old_author").alias("author"),
    coalesce("new_price", "old_price").alias("price"),
    coalesce("new_stock", "old_stock").alias("stock"),
    "flag",
    "message",
    "last_updated"
)

# Write back to Glue table
final_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_dyf")
glueContext.write_dynamic_frame.from_catalog(
    frame=final_dyf,
    database="ecommerce_clean_db",
    table_name="book_status",
    transformation_ctx="write_book_status"
)

job.commit()