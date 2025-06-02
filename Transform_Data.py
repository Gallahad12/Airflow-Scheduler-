from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, row_number
from pyspark.sql.window import Window
# from great_expectations.data_context import FileDataContext
import shutil
import os

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# context = FileDataContext.create(project_root_dir='./')

path_data = "/opt/airflow/data/Supplement_Sales_Weekly_Expanded.csv"

raw_data = spark.read.csv(path_data, header=True, inferSchema=True)

def Add_column_ID(df, ordercolumnby, nameofnewcolumn):
    # Define window
    ordered = Window.orderBy(ordercolumnby)

    # Add row number column
    df = df.withColumn(nameofnewcolumn, row_number().over(ordered))

    # Reorder columns
    cols = [nameofnewcolumn] + [col for col in df.columns if col != nameofnewcolumn]
    return df.select(cols)

raw_data_Clean = Add_column_ID(raw_data,"Date","id")

def save_as_csv(df, output_dir, output_filename):
    tmp_path = os.path.join(output_dir, "_tmp_output")

    # Write to temporary directory
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

    # Find the part file
    for file in os.listdir(tmp_path):
        if file.startswith("part-") and file.endswith(".csv"):
            full_temp_file_path = os.path.join(tmp_path, file)
            break
    else:
        raise FileNotFoundError("CSV part file not found in temp folder.")

    # Move and rename the file
    final_output_path = os.path.join(output_dir, output_filename)
    shutil.move(full_temp_file_path, final_output_path)

    # Clean up temporary folder
    shutil.rmtree(tmp_path)

    print(f"Saved: {final_output_path}")

save_as_csv(raw_data_Clean, "/opt/airflow/data", "Supplement_Sales_Weekly_Expanded_Clean.csv")