# BEES Data Engineering – Breweries Case*

*Objective:*

The goal of this test is to assess your skills in consuming data from an API, transforming and persisting it into a data lake following the medallion architecture with three layers: raw data, curated data partitioned by location, and an analytical aggregated layer.

*Instructions:*

1. *API:* Use the Open Brewery DB API to fetch data. The API has an endpoint for listing breweries: <https://api.openbrewerydb.org/breweries>
2. *Orchestration Tool:* Choose the orchestration tool of your preference (Airflow, Luigi, Mage, etc.) to build a data pipeline. We're interested in seeing your ability to handle scheduling, retries, and error handling in the pipeline.
3. *Language:* Use the language of your preference for the requests and data transformation. Please include test cases for your code. Python and PySpark are preferred but not mandatory.
4. *Containerization:* If you use Docker or Kubernetes for modularization, you'll earn extra points.
5. *Data Lake Architecture:* Your data lake must follow the medallion architecture with a bronze, silver, and gold layer:
    - *Bronze Layer:* Persist the raw data from the API in its native format or any format you find suitable.
    - *Silver Layer:* Transform the data to a columnar storage format such as parquet or delta, and partition it by brewery location. Please explain any other transformations you perform.
    - *Gold Layer:* Create an aggregated view with the quantity of breweries per type and location.
6. *Monitoring/Alerting:* Describe how you would implement a monitoring and alerting process for this pipeline. Consider data quality issues, pipeline failures, and other potential problems in your response.
7. *Repository:* Create a public repository on GitHub with your solution. Document your design choices, trade-offs, and provide clear instructions on how to run your application.
8. *Cloud Services:* If your solution requires any cloud services, please provide instructions on how to set them up. Please *do not post them in your public repository*.

*Evaluation Criteria:*

Your solution will be evaluated based on the following criteria:

1. Code Quality
2. Solution Design  
3. Efficiency
4. Completeness
5. Documentation
6. Error Handling

*Time Frame:*

Please complete the test within 1 week and share the link to your GitHub repository with us.

Remember, the goal of this test is to showcase your skills and approach to building a data pipeline. Good luck!


-------

# Solution

The API data extraction has been build using a containerized local environment pipeline, based on Airflow. Many elements have been implemented and will be explained in detail in the following documentation. First and foremost, it is important to explain how to run the pipeline:

**executing the project:**
1. run `docker build . --tag bees_airflow:latest`
2. after that, run `docker compose up -d`
3. webserver can be accessed in `http://localhost:8080/`
    - user: admin
    - password: admin123
4. DAG can be triggered at any time. For this exercise it will generate tables and alerts based on an "historical ingestion" that I've added to the process. I will explain more about this in later chapters, but to make it production ready is only a matter of changing the previous ingestion from historical to the second to last one.

### Airflow + Docker

Creating a containerized local Airflow data pipeline is very useful for reproducibility. The dockerfile and docker-compose.yml have been created to allow any user with access to this project to simply build it and have it fully running in less than 30 seconds

#### ELEMENTS
1. dockerfile: simple Airflow container creation file
2. docker-compose.yaml: the docker compose file is a little bit more complex, but it mainly creates the necessary variables, services and volumes for Airflow to properly work
    - airflow variables defined in x-environment
    - necessary services: postgres, webserver, scheduler
    - also added an init service to execute some starting commands
    - in order to use the smtp functionality, an airflow.cfg has been created and mounted to the container as well, to properly configure email usage

#### DAG

A DAG called <bees_ingestion_dag> has been created to run the data extraction pipeline. It would certainly be possible to run multiple DAGs for this pipeline, but they would create more complexity to the execution. This DAG has many tasks that execute different parts of the process, more on these bellow.

This DAG has been configured to run everyday at 11pm, rety after 5 seconds any tasks that failed and not to send any emails to users - emails are gonna be sent by their own tasks, if needed.

A little bit more about the emails, they are alerting method of choice for this project. It could be a group in Teams, sms, Whatsapp message, Slack notification, etc. but I thought that the best option for this particular exercise would be emails. I've configured my own mock email as the sender (username and password are already configured in airflow.cfg), so the email tasks should work fine. Only recommendation, in case you want to test this functionality, is to change the emails configured as recipients, in variable `users_email_list` (in dag file)

#### PROJECT STRUCTURE

The project is divided in 3 folders:
1. config: this one holds the airflow.cfg file
2. dags: in here, DAG code in `data`, tasks in `src`, some utility functions in `utils` and tests in `tests`
3. virtualContainer: this folder holds the API data files that are being mounted to the container (more on these files later) and the pipeline generated files, simulating a blob storage

### DATA PIPELINE

Based on exercise requirements, it became clear that the data pipeline should consist of:
1. data extraction (api to landing to bronze)
    - data need to be extracted from https://api.openbrewerydb.org/breweries. Data from this API is generated in JSON format and there are some parameters that can be addedto the endpoint itself
    - I've taken the liberty of changing a little bit the layer structure presented in the exercise text. I've added a LANDING layer before the BRONZE one, mainly to have some sort of simulated iceberg storage for the data extraction. This is useful in many different ways (data redundancy, post processing analysis, increased training dataset for Data Science when implemented, etc.). It also comes at a storage cost, so having it should be well defined during architecture ideation phase. In this case, I thought it would also be nice to have the rawest format possible saved, before it is transformed to columns and rows
    - the extraction phase of the pipeline then becomes: API_to_LANDING > LANDING_to_BRONZE
    - with this separation I was able to insert some tests for the extracted data (as a separated task) and also some alerting tasks
    - when creating the gold table I've noticed a small error in one of the rows (one of the United States countries had a space inthe beginning, which I've fixed in this step)
2. data transformation (silver layer)
    - from here on out the pipeline becomes way more straightforward. Basically for the SILVER layer the breweries table is divided into FACT and DIMENSIONS and partitioned by country (only the FACT)
    - the brewery_types field can be transformed in a key/value pair type of table, it has been implemented in the BRONZE_to_SILVER task
    - the regional partition could be created using states, or even cities, but since the number of records is small, there is no need for that level of partitioning
    - another way to partition the table would be creating some virtual regions (americas, africa + europe, asia, or west coast, east coast or timezone regions, etc.), but this would imply a very good knowledge in geography and again, since the table is not big, thbis level of partioning is not required
    - there are no tests needed for this layer because when data is created here, it has already been tested and considered OK 
3. data loading (gold layer)
    - the best way to create this layer would be to load silver data to a Data Warehouse (PostGres, for example). It would require another container to be created and another task. For the sake of this exercise, I've decided to maintain the final view as a table in the GOLD layer of the virtualContainer, to make it easy to check, verify and present final aggregation

### STEPS

For this exercise to be completed (and improved upon), some steps were created. I've decided to create this initial layer (LANDING) and to create historical data, which will be explained in further details now
1. one important thing to notice is that the API doc is not completely correct. There is an explanation about the brewery_types that don't match with the data extracted from the endpoint. This has been corrected in the `landing_tests` task (and explained there in comments). Within the documentation we can call the total breweries available and understand a little bit about the number of pages and their maximum fields. Doc states that max fields per page is 200, but when data was extracted using 200 items per page and tested, it didn't prove to be right. I've then reverted back the number of elements per page to 50.
2. with the API endpoint and all necessary info, I was able to start extraction. It consists of a simple get request, looped by number of pages in the task `api_to_landing`.
3. one element of Airflow tasks that is really helpful is the `Variable` setting and getting. With variables being set throughout the DAG, tasks can understand and act upon these variable values. A good example is the variable `type_of_error`, which stores the latest error encountered in the DAG and sends its value as part of the alerting email automatically generated, when DAG fails
4. after saving the data, in `JSON` format, in `LANDING` layer, I've created some tests for the data: asserting that the first row has all the fields and that some fields are not null are some examples. More can be found at the `dags/tests/landing_tests.py` file.
5. I've created a json file based on the API data, where I've manually changed some field values, added new different entries and deleted some entries, in order to simulate an historical extraction of the data. This is particularly useful to implement and test the next step (CRUD analysis), as follows:
    - this historical data, since it was manually changed, will be different from any data extraction that happens from now on, allowing us to perform the CRUD analysis and creating an append system for new values ingested
    - the "real" way to create this analysis and monitoring would be comparing today's data with yesterday's - in our use case maybe it would take weeks until some new data was added or updated in order to trigger any CRUD alert, and that is why I've chosen to use "historical" data, so the next elements would always be triggered
    - the code for yesterday's extraction is already in the task's file, making it easy to implement and test the CRUD function with this altered data and then changing it to yesterday's data when everything was working
    - for this particular exercise I won't change historical data back to yesterday's data, so at any DAG run data differences can be analysed and alert triggered
6. if data is considered ok after the tests task, it is then ingested to the `BRONZE` layer.In the `dags/src/landing_to_bronze_data.py` it is possible to see a function called `ingestion_crud`. This function compares the data from the historical extraction with the data that has been ingested and classifies it on a new column called `status`: 
    - identical data (`ok`): rows that existed in the historical extraction and still exist in this new extraction
    - added (`ok`): data that has been added since historical (or yesterday's) data
    - updated (`updated`): data that has been changed (any data in any column other than `id`)
    - deleted (`deleted`): data that has been removed (based on `id`)
7. after analysing the difference in data extractions, a new `BRONZE` table is generated, via append of new elements. It is important to notice that the final table will still contain the rows that have been deleted (marked as `deleted` in `status`) for future references and analysis.
8. with data in `BRONZE` layer, the next step is to do some small transformations and save them in the `SILVER` layer. Basically this table is being divided into FACT and DIMENSIONS. Fact contains id, name, country (for partition) and brewery_type, as type_ids.
9. after this, `GOLD` layer is created with the necessary aggregation
10. at the end (and after every step, due to its trigger rule), if any errors appear in the DAG process, an email is automatically sent to all necessary stakeholders.

UTILS

- I've added to the project two files in the `utils` folder that were used to help me test, fix and debug the code. `dfgui.py` is a helper to display any tables in a GUI (found in https://github.com/bluenote10/PandasDataFrameGUI - I had to perform some fixes to this code but the version in utils is working fine). `read_parquet.py` is a simple code to read and display parquet files.
- When creating the first parquet, I did some tests using Polars instead of Pandas. For this I've added a decorator function `timeit` to some files to know how long specific functions were taking. In the Polars case, for this size of dataset, it didn't perform any better than Pandas, so I've decided to go with Pandas throughout the exercise.





