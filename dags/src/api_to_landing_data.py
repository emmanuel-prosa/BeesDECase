import requests
import math
import json
from datetime import datetime
from airflow.models import Variable

## API variables ##
api_url = "https://api.openbrewerydb.org/v1/breweries"

## Virtual container variables ##
container_path = "/mnt/virtualContainer"
target_layer = "landing"

def api_metadata(api_url, variable_name = "type_of_error"):
    try:
        response = requests.get(f"{api_url}/meta")
        if response.status_code == 200:   # Check if the response status code is 200 (OK)
            json_metadata = response.json()   # Parse JSON response
            print("Metadata fetched successfully!")
            num_of_pages = math.floor(int(json_metadata["total"])/50) + 1
            total_breweries = int(json_metadata["total"])
            print(f"There are {num_of_pages} pages with 50 records each")
            return num_of_pages, total_breweries

    except Exception as e:
        print("Failed to fetch metadata")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Couldn't get metadata from the API")
        raise ValueError(e)


def api_get_data(api_url, num_of_pages, variable_name = "type_of_error"):
    all_data = []
    try:
        for page in range(1, num_of_pages + 1):
            print(f"Fetching page {page}...")
            response = requests.get(f"{api_url}?page={page}")   # Make a GET request to the API endpoint
            
            if response.status_code == 200:   # Check if the response status code is 200 (OK)
                json_data = response.json()   # Parse JSON response
                all_data.extend(json_data)  # Add the current page's data to the overall list
                
            else:
                error_string = f"Error: Failed to retrieve page {page}: {response.status_code}"
                print(error_string)
                Variable.set(variable_name, error_string)
                raise ValueError(error_string)
            
        return all_data
        
    except Exception as e:
        print("Failed to fetch data")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, "Error: Couldn't get data from the API")
        raise ValueError(e)

def api_to_landing_data(json_data, target_layer, variable_name = "type_of_error"):
    try:
        print("Saving data as json file in layer LANDING...")
        with open(f"{container_path}/{target_layer}/{datetime.today().strftime('%Y%m%d')}_data.json", "w") as file:   #creating a file at layer LANDING
            json.dump(json_data, file)
        print("Data saved!")
        file.close()

    except Exception as e:
        print("Failed to save data")
        print(f"An error occurred: {e}")
        Variable.set(variable_name, f"Error: Couldn't save data in layer LANDING: {e}")
        raise ValueError(e)



if __name__ == "__main__":
    num_of_pages, total_breweries = api_metadata(api_url)
    print(total_breweries)
    Variable.set("total_breweries", total_breweries)
    json_data = api_get_data(api_url, num_of_pages)
    if json_data is not None:
        api_to_landing_data(json_data, target_layer)
    else:
        raise ValueError('Error: Data is empty')