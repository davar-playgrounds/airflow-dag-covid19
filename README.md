# airflow-dag-covid19
Basic data pipeline to handle covid19 data sources utilizing Python and Airlflow.

## Setup Steps

1 pip3 install airflow

2 git clone repository on ~/airflow/dags

3 pip install -r requirements.txt

## ETL Design

After defining our data sources and goals, we start the ETL (extract, Transform, Load) design.

To summarize, an ETL process usually consist of three layers:

### Extraction layer:

Takes care of getting data out of the sources, creating a staging area that’s basically mirrors the data on source, but it enables data transformations to be more manageable and predictable.

We’ll be staging extraction data on .csv and pickle, we could use and SQL repository or any other data storage, but this two suits very well for the reality of our processing and data sources.

Pickle its an alternative way to store objects states on python, sometimes might be a better option than .csv for example, in this projection we are staging on both formats for sake of exemplification.

### Transformation layer:

Commonly summarizes the data, adds business rules and dimensions information to the model, aiming mainly on enhancing querying and reporting, providing data with quality and availability.


### Load layer:

Deliveries data to an application, reporting area, … It’s the final step where the data has to be ready for consumption.
