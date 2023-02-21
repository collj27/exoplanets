from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from dalle2 import generate_and_save_image

jupiter_mass = 1.89813e27  # kilograms
earth_mass = 5.9722e24  # kilograms

jupiter_radius = 69911  # km
earth_radius = 6371  # km

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName('DALL-E 2 Image Generation').getOrCreate()

    # read in spark df
    exoplanets_df = spark.read.option("header", True).csv("cleaned_5250.csv")

    exoplanets_df = exoplanets_df.withColumn("mass",
                                             when(col("mass_wrt") == "Jupiter", col("mass_multiplier") * jupiter_mass)
                                             .when(col("mass_wrt") == "Earth", col("mass_multiplier") * earth_mass))

    exoplanets_df = exoplanets_df.withColumn("radius",
                                             when(col("radius_wrt") == "Jupiter",
                                                  col("radius_multiplier") * jupiter_radius)
                                             .when(col("radius_wrt") == "Earth",
                                                   col("radius_multiplier") * earth_radius))

    # TODO: no need to use foreachPartition if theres onlu one of them
    # TODO: drop those will null values

    # dalle limits to 50 requests per minute
    request_limit_per_minute = 50

    # split into partitions of 50
    num_partitions = exoplanets_df.count() / 50
    exoplanets_df = exoplanets_df.repartition(int(num_partitions), col("planet_type"))

    # TODO: submit 50 images async, wait, and then continue
    exoplanets_df.limit(10).foreachPartition(lambda rows: generate_and_save_image(rows))

    # Stop the Spark session
    spark.stop()
