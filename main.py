import json
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("GenericReader").getOrCreate()

def load_config(file_path):
    """
    Load configuration file (JSON format)
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def read_data(spark, config):
    """
    Read data from various file formats based on the config file
    """
    source_path = config['source_path']
    source_type = config['source_type']
    schema = config.get('schema', None)
    
    if source_type == 'csv':
        df = spark.read.csv(source_path, header=True, inferSchema=True if schema is None else False)
    elif source_type == 'parquet':
        df = spark.read.parquet(source_path)
    elif source_type == 'json':
        df = spark.read.json(source_path)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return df

# Example usage:
config_path = "/Users/bharath/Desktop/workspace/pyspark_json/config/config2.json"  # Replace with actual path
config = load_config(config_path)
df = read_data(spark, config)

df.show()
