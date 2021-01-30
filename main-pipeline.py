import boto3
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import Window
import configparser
import os
import re
from launch_redshift_cluster import launch_redshift_cluster
from sql_queries import drop_table_queries, create_table_queries, insert_table_queries, staging_table_copy, \
    staging_table_drop, staging_table_create, staging_table_filter, sql_check_filled

# Set the AWS environment variables (for S3 access)
config = configparser.ConfigParser()
config.read_file(open('credentials.cfg'))
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
s3_bucket = "s3://" + config.get("S3", "S3_BUCKET")


def create_spark_session():
    """ Instantiates a hadoop-based Spark session """
    # Create Spark session with hadoop-aws package
    spark = SparkSession.builder \
        .appName("CapstonePipeline") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def to_spark_friendly_json(spark, filename):
    """
    Reads the input files as text, fixes a number of values that cause corruption errors when read by spark from json,
    and saves the output to text files in an S3 bucket.
     - converts python False, True and None values to JSON equivalents
     - removes the u in front of unicode strings
     - replaces ASCII encoded values by their unicode equivalent
     """
    text_df = spark.read.text(s3_bucket + "/" + filename + ".json")
    text_df = (text_df.withColumn('value', F.regexp_replace('value', 'False', 'false'))
               .withColumn('value', F.regexp_replace('value', 'True', 'true'))
               .withColumn('value', F.regexp_replace('value', 'None', 'null'))
               .withColumn('value', F.regexp_replace('value', "u'", "'"))
               .withColumn('value', F.regexp_replace('value', "u\"", "\""))
               .withColumn('value', F.regexp_replace('value', r"\\x", r"\\u00")))
    text_df.write.mode("overwrite").text(s3_bucket + "/fixed/" + filename)


def process_places(spark):
    """ Loads, cleans and enriches the places dataset:
    - loads the json dataset from S3
    - reformats the dataset into Spark friendly json
    - identifies US-based businesses
    - adds a population column from the us population by postcode csv file
    - saves the dataset in S3 for further processing
    """
    to_spark_friendly_json(spark, "places")
    # about 13000 corrupted records, with no obvious pattern to be fixed. Can be dropped.
    places_df = spark.read.json(s3_bucket + "/fixed/places", mode="DROPMALFORMED")
    places_df = places_df.drop("phone", "closed", "gps", "hours")

    def extract_usa(address):
        """" Takes an array representing an address as input and identifies US addresses by recognising a US zipcode
        pattern. Returns a string containing the State code and zipcode. e.g. CA 90210 """
        # if last string in the address contains two isolated capital letters followed by 5 digits,
        # then the country is USA
        match = re.search("^[A-Z]{2}\s\d{5}", address[-1].split(sep=', ')[-1])
        if match is None:
            return None
        else:
            return match.group()

    udf_extract_usa = F.udf(extract_usa, StringType())
    places_df = places_df.withColumn("state_and_zip_usa", udf_extract_usa(places_df.address))
    places_df = (places_df
                 .withColumn("state", F.split(places_df["state_and_zip_usa"], ' ').getItem(0))
                 .withColumn("postcode", F.split(places_df["state_and_zip_usa"], ' ').getItem(1))
                 .drop("state_and_zip_usa"))
    places_df = places_df.withColumn("country", F.when(places_df.state.isNull(), None).otherwise("USA"))
    places_df = places_df.withColumn("address", F.concat_ws(",", places_df.address))

    # Convert price to integer index - Spark automatically converts the price string to an integer data type. Nice!
    places_df = places_df.withColumn("price", F.length(places_df.price))
    # Add population column to the dataset
    pop_df = spark.read.csv(s3_bucket + "/us_population_by_zipcode.csv", header=True)
    pop_df = (pop_df.groupby("Zip Code ZCTA")
                    .agg({"2010 Census Population": "sum"})
                    .select(F.col("sum(2010 Census Population)").alias("population"), "Zip Code ZCTA")
              )
    places_df = (places_df.join(F.broadcast(pop_df), pop_df['Zip Code ZCTA'] == places_df['postcode'], how="left")
                          .drop('Zip Code ZCTA')
                 )
    places_df.write.json(s3_bucket + "/prepped/places")
    return None


def process_reviews(spark):
    """ Loads, cleans and enriches the reviews dataset:
    - loads the json dataset from S3
    - reformats the dataset into Spark friendly json
    - derives the day of the week from the review timestamp
    - derives a night or day flag from the review timestamp
    - derives the reviewer's average rating
    - saves the dataset in S3 for further processing
    """
    to_spark_friendly_json(spark, "reviews")
    reviews_df = spark.read.json(s3_bucket + "/fixed/reviews", mode='DROPMALFORMED')
    # Convert unixReviewtime from long datatype to unix timestamp
    reviews_df = (reviews_df.drop("reviewerName")
                  .withColumn('reviewTime', F.from_unixtime(reviews_df.unixReviewTime).cast(TimestampType()))
                  # .withColumn('timeOfReview', reviews_df['timeOfReview'].cast(TimestampType()))
                  .withColumn('weekday', F.date_format(F.col("reviewTime"), "E")))
    reviews_df = (reviews_df
                  .withColumn('day_night',
                              F.when((F.hour(reviews_df.reviewTime) < 8) | (F.hour(reviews_df.reviewTime) >= 18),
                                     'night')
                              .when((F.hour(reviews_df.reviewTime) >= 8) | (F.hour(reviews_df.reviewTime) < 18), 'day')
                              .otherwise(None))
                  # derive the user's average review rating to a new column
                  .withColumn("userAvgRating", F.avg("rating").over(Window.partitionBy("gPlusUserId")))
                  .drop("unixReviewTime")
                  )
    reviews_df.write.json(s3_bucket + "/prepped/reviews")
    return None


def create_data_lake(spark):
    """ Joins the reviews and places datasets into one large Data Lake containing only US-based reviews and
    loads it to S3 as parquet files partitioned by business category. Also filters out the review text and loads a
    non-partitioned staging data lake into S3 for further processing. """

    reviews_df = spark.read.json(s3_bucket + "/prepped/reviews")
    places_df = spark.read.json(s3_bucket + "/prepped/places")
    # Drop reviews with missing categories
    reviews_df = reviews_df.filter(reviews_df.categories.isNotNull())
    # Keep only USA places
    places_df = places_df.filter(places_df.country == "USA")
    # Join reviews with places into a single dataset
    lake = reviews_df.join(places_df, "gPlusPlaceId", how="inner")
    # Check join was successful and no larger than expected
    reviews_count = reviews_df.count()
    lake_count = lake.count()
    if lake_count > reviews_count:
        raise ValueError("Join resulted in too many rows")

    # Flatten categories column
    lake = (lake.withColumn("categories", F.explode(F.col("categories")))
                .withColumnRenamed("categories", "category")
            )
    # Filter out reviews for rare categories
    pre_filter_count = lake.select("category").distinct().count()
    lake = lake.withColumn("num_reviews_for_cat", F.count("gPlusPlaceId").over(Window.partitionBy("category")))
    lake = (lake.filter(lake.num_reviews_for_cat > 10)
                .drop("num_reviews_for_cat")
            )
    post_filter_count = lake.select("category").distinct().count()
    if post_filter_count >= pre_filter_count:
        raise ValueError("Categories were incorrectly filtered")

    # Write to S3
    lake.write.partitionBy('category').parquet(s3_bucket + "/reviews_lake")
    lake.select("gPlusPlaceId", "category", "day_night", "gPlusUserId", "rating", "reviewTime",
                "userAvgRating", "weekday", "address", "country", "name", "population", "postcode",
                "price", "state").write.parquet(s3_bucket + "/temp_lake")

    # Check data was written out to parquet files
    reviews_lake = spark.read.parquet(s3_bucket + "/reviews_lake")
    count_reviews = reviews_lake.count()
    temp_lake = spark.read.parquet(s3_bucket + "/temp_lake")
    count_temp = temp_lake.count()
    if count_reviews == 0 or count_temp == 0:
        raise ValueError("Data lakes were not populated")


def load_staging_table(cur, conn, arn):
    """ Loads a staging table in Redshift from an S3 Data Lake"""
    # drop staging table if exists
    print("Dropping staging table")
    try:
        cur.execute(staging_table_drop)
        conn.commit()
    except Exception as err:
        print(err)
    # create fresh staging table
    print("Creating staging table")
    try:
        cur.execute(staging_table_create)
        conn.commit()
    except Exception as e:
        print(e)
    # copy staging table from full s3 datalake
    print("Copying staging table from S3 location: {}".format(s3_bucket + "/temp_lake"))
    try:
        cur.execute(staging_table_copy.format(s3_bucket + "/temp_lake", arn))
        conn.commit()
    except Exception as e:
        print(e)
    # filter staging table to keep only the relevant categories
    print("Filtering irrelevant categories from staging table")
    try:
        cur.execute(staging_table_filter)
        conn.commit()
    except Exception as err:
        print(err)


def load_final_tables(cur, conn):
    """ Loads the fact and dimension tables with data from the staging table """
    # drop all Redshift tables if they exist already
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)
    # create final table for star schema
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)
    # load tables
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)


def check_data_quality(cur, tables):
    """ Takes a list of table names as input and checks that they are not empty """
    # Check for empty tables
    for table in tables:
        cur.execute(sql_check_filled.format(table))
        rows = cur.fetchall()
        if len(rows) == 0:
            raise ValueError(f"Data quality check failed. Table {table} is empty")
        else:
            print(f"Data quality check passed, {table} is filled")


def drop_staging_table(cur, conn):
    try:
        cur.execute(staging_table_drop)
        conn.commit()
    except Exception as err:
        print(err)


def clear_temp_data():
    """ Connects to s3 and deletes the fixed input files as well as the temporary data lake """
    # Clear temp data lake from s3
    s3 = boto3.client('s3')
    bucket = config.get("S3", "S3_BUCKET")
    temp_lake = s3.list_objects_v2(Bucket=bucket, Prefix="temp_lake/")
    for file in temp_lake['Contents']:
        print('Deleting', file['Key'])
        s3.delete_object(Bucket=bucket, Key=file['Key'])
    # Clear temp cleaned input files from s3
    fixed_files = s3.list_objects_v2(Bucket=bucket, Prefix="fixed/")
    for file in fixed_files['Contents']:
        print('Deleting', file['Key'])
        s3.delete_object(Bucket=bucket, Key=file['Key'])


def main():
    """ Instantiates a spark session then executes the data pipeline:
     - converts the input files to spark friendly json
     - transforms, filters and enriches the reviews and places data
     - creates two data lakes:
            - a reviews data lake that contains reviews for all the places in the USA, partitioned by category
            - a temporary data lake (unpartitioned) used to load a Redshift table
     - launches a Redshift cluster
     - creates a database of USA restaurant reviews
     - checks database tables have been filled
     - clears the temporary files and data lake
     """
    spark = create_spark_session()
    process_places(spark)
    process_reviews(spark)
    create_data_lake(spark)
    spark.stop()  # ensures the application isn't left hanging at the end of the batch spark job

    host, arn = launch_redshift_cluster()
    # host = "dwhcluster2.ckbqb5g8ifko.eu-west-1.redshift.amazonaws.com"
    # arn = "arn:aws:iam::618534798858:role/dwhRole"
    # Connect to the Redshift database cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host,
                                                                                   config.get("DWH", "DWH_DB"),
                                                                                   config.get("DWH", "DWH_DB_USER"),
                                                                                   config.get("DWH", "DWH_DB_PASSWORD"),
                                                                                   config.get("DWH", "DWH_PORT"))
                            )
    cur = conn.cursor()
    print("Connected to Redshift")
    load_staging_table(cur, conn, arn)
    load_final_tables(cur, conn)
    tables = ["reviews", "places", "population", "users"]
    check_data_quality(cur, tables)
    drop_staging_table(cur, conn)
    clear_temp_data()


if __name__ == "__main__":
    main()
