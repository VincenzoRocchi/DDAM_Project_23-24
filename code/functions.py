from pyspark.sql.functions import isnan, when, count, col, isnull
from tqdm import tqdm

def check_nan(spark, column_to_check="", label_only=False):
    # List of file numbers
    file_numbers = [1, 3, 9, 20, 21, 34, 35, 42, 44, 48, 60]
    
    for number in tqdm(file_numbers, desc="Processing files", unit="file"):
    
        file_path = r"C:\Users\Vincenzo\Projects\DDAM_data\malware\CTU-IoT-Malware-Capture-{}-1conn.log.labeled.csv".format(number)
        df = spark.read.option("escape", "\"").option("delimiter", "|").csv(file_path, header='true', inferSchema='true')
        df = df.toDF(*(c.replace('.', '_') for c in df.columns))

        print("", flush=True)
        # Check for missing values
        if label_only:
            missing = df.select([count(when(isnull("detailed-label"), "detailed-label")).alias("missing_count")])
            print("Missing values in file {}: ".format(number))
            missing.select("missing_count").show()

        elif column_to_check:
            missing = df.select([count(when(isnull(column_to_check), column_to_check)).alias("missing_count")])
            print("Missing values in file {}: ".format(number))
            missing.select("missing_count").show()

        elif column_to_check == "all":
            missing = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
            print("Missing values in file {}: ".format(number))
            missing.show()

        else:
            print("Please enter a valid column name or enter True to check the label column only")

from pyspark.sql.functions import col, countDistinct
from tqdm import tqdm

def count_distinct_values(df):
    result = {}
    columns = df.columns

    # Use tqdm to create a progress bar
    for column in tqdm(columns, desc="Counting Distinct Values", unit="column"):
        distinct_count = df.select(column).agg(countDistinct(column)).collect()[0][0]
        result[column] = distinct_count

    return result

        # distinct_counts = count_distinct_values(df)

        # for column, count in distinct_counts.items():
        #     print(f"Column '{column}' has {count} distinct values.")


if __name__ == "__main__":
    pass