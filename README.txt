# Yellow Taxi Trip Data Analysis

## Project Overview:
This project extracts one month of yellow taxi trip data, cleans it by filtering out records with `passenger_count <= 0`, loads the clean data into a SQL database, and then aggregates the data to visualize the total amount per day.

## Setup Instructions:
1. Clone this repository to your local machine.
2. Install the required libraries using `pip install -r requirements.txt`.
3. Update the `config.yaml` file with your database connection details.
4. Create the database specified in the configuration file (`ota`) if it doesn't exist.

## Execution Instructions:
1. Run the Python script (`main.py`) to execute the data processing pipeline.
2. Navigate to the `output` folder to find the generated visualization(s) and check the database for the loaded data.

## Discussion:
I encountered an issue with loading the data into the database, which I resolved using a chunking approach in the Python script. This ensured efficient processing and prevented memory errors.

## Outputs:
![Total Amount Per Day](./aggregates_screenshots/total_amount_per_day.png)
