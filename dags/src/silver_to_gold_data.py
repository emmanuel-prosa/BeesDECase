from functools import wraps
import time

import pandas as pd
import pandasql as ps

from airflow.models import Variable


container_path = "/mnt/virtualContainer"
extraction_layer = "silver"
loading_layer = "gold"

# Path to the SILVER parquet files
parquet_silver_file_path = f"{container_path}/{extraction_layer}"

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
def aggregation_table(container_path, loading_layer, variable_name = "type_of_error"):
    fact_table = pd.read_parquet(f"{parquet_silver_file_path}/breweries_fact.parquet")
    type_dim = pd.read_parquet(f"{parquet_silver_file_path}/type_dim.parquet")
    address_dim = pd.read_parquet(f"{parquet_silver_file_path}/address_dim.parquet")

    try:
        # Creating a base dataframe to extract necessary data
        # it is important to notice that, even if I'm joining back dimensions to the fact, I'm still saving some procerssing power by not using additional_dim 
        merged_1DF = pd.merge(fact_table, address_dim, how="left", on="id", suffixes=("_f", ""))[[col for col in fact_table.columns if col != 'country'] + [col for col in address_dim.columns if col != 'id']]
        merged_2DF = pd.merge(merged_1DF, type_dim, how="left", left_on="brewery_type", right_on="type_id", suffixes=("_f", ""))[[col for col in merged_1DF.columns if col != 'brewery_type'] + [col for col in type_dim.columns if col != 'type_id']]

        query = """
            SELECT 
                country
                ,brewery_type
                ,COUNT(id) AS num_of_breweries
            FROM merged_2DF
            GROUP BY country, brewery_type
        """
        result = ps.sqldf(query, locals())
        result.to_parquet(f"{container_path}/{loading_layer}/agg_view.parquet", index=False)

    except Exception as e:
        print("Failed to create gold table")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Failed to create gold table")
        raise ValueError(e)

if __name__ == "__main__":
    aggregation_table(container_path, loading_layer)
    
