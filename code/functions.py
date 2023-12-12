from pyspark.sql.functions import isnan, when, count, col, isnull

def check_nan(spark, label_ony=False):

    # List of file numbers
    file_numbers = [1, 3, 9, 20, 21, 34, 35, 42, 44, 48, 60]
    # Loop through the file numbers
    for number in file_numbers:
        # Construct the file path
        file_path = r"C:\Users\Vincenzo\Projects\DDAM_data\malware\CTU-IoT-Malware-Capture-{}-1conn.log.labeled.csv".format(number)

        # Read the CSV file
        df = spark.read.option("escape", "\"").option("delimiter", "|").csv(file_path, header='true', inferSchema='true')

        # Rename columns with underscores
        df = df.toDF(*(c.replace('.', '_') for c in df.columns))

        # Check for missing values
        

        if label_ony:
            missing = df.select([count(when(isnull("detailed-label"), "detailed-label")).alias("missing_count")])
            print("Missing values in file {}: ".format(number))
            missing.select("missing_count").show()
        else:
            missing = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
            print("Missing values in file {}: ".format(number))
            missing.show()

if __name__ == "__main__":
    pass