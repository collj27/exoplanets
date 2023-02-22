from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from dalle2 import generate_and_save_images

jupiter_mass = 1.89813e27  # kilograms
earth_mass = 5.9722e24  # kilograms

jupiter_radius = 69911  # km
earth_radius = 6371  # km
#TODO: figure out billing hard limit and look into making buckets per planet type to make ui easier
if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName('DALL-E 2 Image Generation').getOrCreate()

    # read in spark df
    exoplanets_df = spark.read.option("header", True).csv("cleaned_5250.csv")

    # calculate mass
    exoplanets_df = exoplanets_df.withColumn("mass",
                                             when(col("mass_wrt") == "Jupiter", col("mass_multiplier") * jupiter_mass)
                                             .when(col("mass_wrt") == "Earth", col("mass_multiplier") * earth_mass))
    # calculate radius
    exoplanets_df = exoplanets_df.withColumn("radius",
                                             when(col("radius_wrt") == "Jupiter",
                                                  col("radius_multiplier") * jupiter_radius)
                                             .when(col("radius_wrt") == "Earth",
                                                   col("radius_multiplier") * earth_radius))

    # dalle limits to 50 requests per minute
    request_limit_per_minute = 50

    # split into partitions of 50
    num_partitions = exoplanets_df.count() / 50
    exoplanets_df = exoplanets_df.repartition(int(num_partitions), col("planet_type"))

    # loop through partitions
    exoplanets_df.foreachPartition(lambda rows: generate_and_save_images(rows))

    # Stop the Spark session
    spark.stop()
