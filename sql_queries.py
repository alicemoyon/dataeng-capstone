import configparser

# Retrieve parameters
config = configparser.ConfigParser()
config.read('credentials.cfg')

# Drop tables
staging_table_drop = "DROP TABLE IF EXISTS staging"
users_table_drop = "DROP TABLE IF EXISTS users"
places_table_drop = "DROP TABLE IF EXISTS places"
population_table_drop = "DROP TABLE IF EXISTS population"
reviews_table_drop = "DROP TABLE IF EXISTS reviews"

# Create staging table
staging_table_create = ("""
        CREATE TABLE staging (
            gPlusPlaceId TEXT,
            category TEXT,
            day_night TEXT,
            gPlusUserId TEXT,
            rating DOUBLE PRECISION,
            reviewTime TEXT,
            userAvgRating DOUBLE PRECISION,
            weekday TEXT,
            address TEXT,
            country TEXT,
            name TEXT,
            population DOUBLE PRECISION,
            postcode TEXT,
            price BIGINT,
            state TEXT);
""")

# Load staging table
staging_table_copy = ("""COPY staging FROM '{}'
                            IAM_ROLE '{}'
                            FORMAT AS PARQUET;"""
                      )

staging_table_filter = (""" DELETE FROM staging
                            WHERE category NOT LIKE '%{}%'; """
                        ).format(config.get("PARAMS", "CATEGORY"))

# Create final tables

users_table_create = ("""
        CREATE TABLE users (
            gPlusUserId TEXT UNIQUE DISTKEY SORTKEY,
            avgRating DECIMAL);
""")

places_table_create = ("""
        CREATE TABLE places (
            gPlusPlaceId TEXT UNIQUE DISTKEY,
            name TEXT,
            price INT,
            state TEXT,
            postcode TEXT)
            SORTKEY(gPlusPlaceId, postcode);
""")

population_table_create = ("""
        CREATE TABLE population (
            postcode TEXT DISTKEY,
            population DECIMAL);
""")

reviews_table_create = ("""
        CREATE TABLE reviews (
            gPlusPlaceId TEXT DISTKEY SORTKEY REFERENCES places(gPlusPlaceId),
            gPlusUserId TEXT REFERENCES users(gPlusUserId),
            category TEXT,
            rating DECIMAL,
            weekday TEXT,
            day_night TEXT);
""")

# Load final tables

users_table_insert = ("""INSERT INTO users (gPlusUserId, avgRating)
                            SELECT DISTINCT gPlusUserId, userAvgRating
                            FROM staging
                            ;""")

places_table_insert = ("""INSERT INTO places (gPlusPlaceId, name, price, state, postcode)
                            SELECT DISTINCT gPlusPlaceId, name, price, state, postcode
                            FROM staging
                            ;""")

population_table_insert = ("""INSERT INTO population (postcode, population)
                            SELECT DISTINCT postcode, population
                            FROM staging
                            ;""")

reviews_table_insert = ("""INSERT INTO reviews (gPlusPlaceId, gPlusUserId, category, rating, weekday, day_night)
                            SELECT gPlusPlaceId, gPlusUserId, category, rating, weekday, day_night
                            FROM staging
                            ;""")

# Data quality checks
sql_check_filled = "SELECT COUNT(*) FROM public.{}"

# Query lists
create_table_queries = [users_table_create, places_table_create, population_table_create, reviews_table_create]
drop_table_queries = [reviews_table_drop, users_table_drop, places_table_drop, population_table_drop]
insert_table_queries = [users_table_insert, places_table_insert, population_table_insert, reviews_table_insert]
