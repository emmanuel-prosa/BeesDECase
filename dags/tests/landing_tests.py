import os
import json
from datetime import datetime

from airflow.models import Variable

## Virtual container variables ##
container_path = "/mnt/virtualContainer"
target_layer = "landing"
landing_path = f"{container_path}/{target_layer}"

## Data variables ##
total_breweries = Variable.get("total_breweries")

## Latest data file for tests ##
latest_extracted_file = f"{datetime.today().strftime('%Y%m%d')}_data.json"
with open(f"{landing_path}/{latest_extracted_file}", "r") as file:
    latest_data = json.load(file)
file.close()


## initial tests ##
# assertion of data existence and structure validation
def initial_tests(latest_data, variable_name = "type_of_error"):

    # Check if the data is a list
    assert isinstance(latest_data, list), "Expected latest_data to be a list"

    if len(latest_data) > 0:
        first_item = latest_data[0]
        try:
            # Validate that the first item in the list contains specific fields
            assert "id" in first_item, 'Key "id" not found in the first item'
            assert "name" in first_item, 'Key "name" not found in the first item'
            assert "brewery_type" in first_item, 'Key "brewery_type" not found in the first item'
            assert "address_1" in first_item, 'Key "address_1" not found in the first item'
            assert "address_2" in first_item, 'Key "address_2" not found in the first item'
            assert "address_3" in first_item, 'Key "address_3" not found in the first item'
            assert "city" in first_item, 'Key "city" not found in the first item'
            assert "state_province" in first_item, 'Key "state_province" not found in the first item'
            assert "postal_code" in first_item, 'Key "postal_code" not found in the first item'
            assert "country" in first_item, 'Key "country" not found in the first item'
            assert "longitude" in first_item, 'Key "longitude" not found in the first item'
            assert "latitude" in first_item, 'Key "latitude" not found in the first item'
            assert "phone" in first_item, 'Key "phone" not found in the first item'
            assert "website_url" in first_item, 'Key "website_url" not found in the first item'
            assert "state" in first_item, 'Key "state" not found in the first item'
            assert "street" in first_item, 'Key "street" not found in the first item'

            # Checking the data types and nullable values
            assert isinstance(first_item["id"], str), '"id" should be and string and not null'
            assert isinstance(first_item["name"], str), '"name" should be and string and not null'
            assert isinstance(first_item["brewery_type"], str), '"brewery_type" should be and string and not null'
            assert isinstance(first_item["address_1"], str), '"address_1" should be and string and not null'
            assert isinstance(first_item["city"], str), '"city" should be and string and not null'
            assert isinstance(first_item["country"], str), '"country" should be and string and not null'
            assert isinstance(first_item["state"], str), '"state" should be and string and not null'

        except AssertionError as e:
            Variable.set(variable_name, e)
            raise ValueError(e)


## following tests ##
# assertion of primary key uniqueness and column value consistency     
def secondary_tests(latest_data, total_breweries, variable_name = "type_of_error"):
    list_of_ids = [item["id"] for item in latest_data]
    list_of_brewery_types = [item["brewery_type"] for item in latest_data]
    list_of_names = [item["name"] for item in latest_data  if item["name"] is not None]

    try:
        # Checking if ids are unique
        assert len(list_of_ids) == len(set(list_of_ids)), 'There are duplicated ids in the data'    

        # Checking if brewery types are within predefined list (per API docs - https://www.openbrewerydb.org/documentation/)
        # FIX: documentation is outdated, so I've added the missing elements (taproom, beergarden and location) to the list so this error is mitigated
        assert set(list_of_brewery_types) == set(["beergarden", "location", "taproom", "micro", "nano", "regional", "brewpub", "large", "planning", "bar", "contract", "proprietor", "closed"]), 'Brewery types are mismatched'

        # Verifying if number of rows is equal to total breweries
        # FIX: documentation states that max per_page is 200, but it is actually 50. After running this test I've reverted num of pages calculation back to 50 entries per page
        assert len(latest_data) == int(total_breweries), 'Extracted data does not have the correct number of elements'

        # Verifying if there are any NULL names
        assert len(latest_data) == len(list_of_names), 'Extracted data has NULL names'

    except AssertionError as e:
        Variable.set(variable_name, e)
        raise ValueError(e)

if __name__ == "__main__":
    initial_tests(latest_data)
    secondary_tests(latest_data, total_breweries)
