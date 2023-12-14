import pyspark
from tqdm import tqdm
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Malware") \
    .config("spark.executor.memory", "16g") \
    .master('local[*]') \
    .getOrCreate()

data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["Name", "Value"])
df.show()