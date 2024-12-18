import os
from functools import wraps
import time
from datetime import datetime

import pandas as pd

from airflow.models import Variable


container_path = "/mnt/virtualContainer"
extraction_layer = "bronze"
loading_layer = "silver"

# Path to the BRONZE parquet file
parquet_bronze_file_path = f"{container_path}/{extraction_layer}/breweries_data.parquet"


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__}{args} {kwargs} took {total_time:.4f} seconds")
        return result
    return timeit_wrapper

@timeit
def data_modelling(parquet_bronze_file_path, container_path, loading_layer, variable_name = "type_of_error"):
    bronzeDF = pd.read_parquet(parquet_bronze_file_path)
    try: 
        # Creating ids for different types
        types_dict = {
        0 : "beergarden", 
        1 : "location", 
        2 : "taproom", 
        3 : "micro", 
        4 : "nano", 
        5 : "regional", 
        6 : "brewpub", 
        7 : "large", 
        8 : "planning", 
        9 : "bar", 
        10 : "contract", 
        11 : "proprietor", 
        12 : "closed",
        }
        # Create a reverse dictionary mapping values to keys - to substitute in the fact table
        reverse_dict = {v: k for k, v in types_dict.items()}
        bronzeDF['brewery_type'] = bronzeDF['brewery_type'].replace(reverse_dict)

        # Creating fact and dimension tables
        factDF = bronzeDF[["id", "name", "country", "brewery_type", "status"]]
        type_dimDF = pd.DataFrame(list(types_dict.items()), columns=["type_id", "brewery_type"])
        address_dimDF = bronzeDF[["id", "country", "state", "state_province", "city", "postal_code", "address_1", "address_2", "address_3", "street"]]
        additional_dimDF = bronzeDF[["id", "phone", "website_url", "latitude", "longitude", ]]

        # Path to the SILVER parquet file
        parquet_silver_file_path = f"{container_path}/{loading_layer}"

        # Save DataFrames as Parquet files
        factDF.to_parquet(f"{parquet_silver_file_path}/breweries_fact.parquet", index=False, partition_cols=["country"])
        type_dimDF.to_parquet(f"{parquet_silver_file_path}/type_dim.parquet", index=False)
        address_dimDF.to_parquet(f"{parquet_silver_file_path}/address_dim.parquet", index=False)
        additional_dimDF.to_parquet(f"{parquet_silver_file_path}/additional_dim.parquet", index=False)

    except Exception as e:
        print("Failed to create silver tables")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Failed to create silver tables")
        raise ValueError(e)

if __name__ == "__main__":
    data_modelling(parquet_bronze_file_path, container_path, loading_layer)
    
