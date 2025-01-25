from __future__ import print_function

import os
import shutil
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

# Christopher Liu CS777
# Using python HW template and functions


# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True

    except ValueError:
        return False


# Function - Cleaning
# For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles,
# fare amount and total amount are more than 0 dollars
def correctRows(row):
    if len(row) == 17:  # Check if row has the correct number of fields
        if (isfloat(row[16]) and isfloat(row[15]) and float(
                row[15]) > 0):  # Ensure total money and trip time are valid
            return True
    return False


# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")

    rdd = sc.textFile(sys.argv[1])
    # Split each line into a list of values based delineated on commas
    rows = rdd.map(lambda line: line.split(","))
    # our cleaned rows after applying helper function
    cleaned_rows = rows.filter(correctRows)

    # Housekeeping, pycharm threw errors when I tried to run script more than once
    # Remove existing output directories if they exist
    if os.path.exists(sys.argv[2]):
        shutil.rmtree(sys.argv[2])
    if os.path.exists(sys.argv[3]):
        shutil.rmtree(sys.argv[3])

    # Task 1 Helpers
    # might utilize lambda functions later homeworks
    def get_taxi_driver_pairs(row):
        # Extract taxi ID and driver ID as a pair.
        return row[0], row[1]

    def count_drivers(pair):
        # Count drivers for each taxi. start at 1
        return pair[0], 1

    # Task 1 Implementation
    # unique drivers is a set of taxi id and driver id pairs
    unique_driver = cleaned_rows.map(get_taxi_driver_pairs).distinct()
    # count drivers per taxi
    num_taxi_driver = unique_driver.map(count_drivers).reduceByKey(add)

    # Get the top 10 taxis by driver count
    top_10_taxis = num_taxi_driver.top(10, key=lambda x: x[1])
    # return back to rdd format
    results_task1 = sc.parallelize(top_10_taxis)
    # save txt file
    results_task1.coalesce(1).saveAsTextFile(sys.argv[2])

    # Task 2 Helpers
    def get_driver_earnings(row):
        # Extract driver ID, earnings, and trip time.
        return row[1], (float(row[16]), float(row[15]))


    def sum_earnings_and_time(a, b):
        # Aggregate total earnings and trip times.
        return a[0] + b[0], a[1] + b[1]


    def calculate_earnings_per_minute(values):
        # Calculate average earnings per minute.
        total_earnings, total_time = values
        return total_earnings / total_time


    # Task 2 Implementation
    driver_earnings = cleaned_rows.map(get_driver_earnings)
    # use reducebykey to aggregate earnings and time for each driver
    total_earnings_and_time = driver_earnings.reduceByKey(sum_earnings_and_time)
    # average earning per min for each driver
    average_earnings_per_minute = total_earnings_and_time.mapValues(calculate_earnings_per_minute)

    # Get the top 10 drivers by earnings calculated to minute
    top_10_earners = average_earnings_per_minute.top(10, key=lambda x: x[1])
    results_task2 = sc.parallelize(top_10_earners)
    results_task2.coalesce(1).saveAsTextFile(sys.argv[3])

    # Print results
    print("Results Task 1 - Top 10 Active Taxis(ID):")
    for taxi, count in top_10_taxis:
        print(f"Taxi ID: {taxi}, Number of Drivers: {count}")

    print("\nResults Task 2 - Top 10 Best Earning Drivers (Earnings Per Minute):")
    for driver, earnings_per_minute in top_10_earners:
        print(f"Driver ID: {driver}, Average Earnings Per Minute: ${earnings_per_minute:.2f}")

    # stop spark
    sc.stop()
