import os
from functools import wraps
import time
from datetime import datetime

import pandas as pd

from airflow.models import Variable


container_path = "/mnt/virtualContainer"
extraction_layer = "landing"
loading_layer = "bronze"

## Historical extraction ##
hist_file = "19000101_data.json"

## Previous extraction ##
if len(os.listdir(f"{container_path}/{extraction_layer}")) > 1:
    previous_extracted_file = f"{max([int(item.split('_')[0]) for item in os.listdir(f'{container_path}/{extraction_layer}') if int(item.split('_')[0]) != int(datetime.today().strftime('%Y%m%d'))])}_data.json" 
else: 
    previous_extracted_file = None

# Path to the JSON file
json_file_path = f"{container_path}/{extraction_layer}/{datetime.today().strftime('%Y%m%d')}_data.json"
# Path to the historical file
json_hist_path = f"{container_path}/{extraction_layer}/{hist_file}"
# Path to the previous extraction file
json_previous_path = f"{container_path}/{extraction_layer}/{previous_extracted_file}"


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
def ingestion_crud(json_hist_path, json_file_path, variable_name = "type_of_error"):
    try:
        # Creating necessary Dataframes
        histDF = pd.read_json(json_hist_path)
        currentDF = pd.read_json(json_file_path)

        # Items that have been added
        joinedDF = pd.merge(histDF, currentDF, how="right", on="id", suffixes=("_hist", ""))
        addedDF = joinedDF[joinedDF["name_hist"].isnull()][currentDF.columns]

        # Items that have been deleted
        joinedDF = pd.merge(histDF, currentDF, how="left", on="id", suffixes=("", "_current"))
        deletedDF = joinedDF[joinedDF["name_current"].isnull()][histDF.columns]

        # Items that have been updated
        notDeletedDF = histDF[~histDF["id"].isin(deletedDF["id"])]
        joinedDF = pd.merge(notDeletedDF, currentDF, how="left", on="id", suffixes=("_hist", ""))[currentDF.columns]
        diffDF = histDF[~histDF["id"].isin(deletedDF["id"])].compare(joinedDF, align_axis=0).xs('other', level=1, drop_level=False)   
        updatedDF = joinedDF[joinedDF.index.isin([index[0] for index in diffDF.index.values.tolist()])]

        return addedDF, deletedDF, updatedDF

    except Exception as e:
        print("Failed to create bronze CRUD report")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Failed to create bronze CRUD report")
        raise ValueError(e)


@timeit
def ingestion_extraction(json_hist_path, json_file_path, container_path, loading_layer, variable_name = "type_of_error"):
    try:
        # Identify CRUD operations on new data
        addedDF, deletedDF, updatedDF = ingestion_crud(json_hist_path, json_file_path)

        histDF = pd.read_json(json_hist_path)
        histDF["status"] = "ok"

        # Marking deleted rows in table
        histDF.loc[histDF["id"].isin(deletedDF["id"]), "status"] = "deleted"

        # Marking updated rows in table
        histDF.loc[histDF["id"].isin(updatedDF["id"])] = updatedDF.copy()
        histDF.loc[histDF["id"].isin(updatedDF["id"]), "status"] = "updated"

        # Adding rows to the table
        addedDF["status"] = "ok"

        # Generating final updated version (already with CRUD status)
        bronzeDF = pd.concat([histDF, addedDF])

        #Fixing one small column defect
        bronzeDF['country'] = bronzeDF['country'].str.lstrip()

        # Path to save the Parquet file
        parquet_file_path = f"{container_path}/{loading_layer}/breweries_data.parquet"

        # Save the DataFrame as a Parquet file
        bronzeDF.to_parquet(parquet_file_path, index=False)

        Variable.set("deleted", len(deletedDF))
        Variable.set("updated", len(updatedDF))
        Variable.set("inserted", len(addedDF))
        print(f"Data has been saved to layer BRONZE using Pandas")

    except Exception as e:
        print("Failed to create bronze tables")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Failed to create bronze tables")
        raise ValueError(e)



if __name__ == "__main__":
    ingestion_extraction(json_hist_path, json_file_path, container_path, loading_layer)
