Usage:
To run this project, navigate to the root folder and run the following commands.


To extract, transform, load, and save time series data from data-science db:

$ python3 run.py etl


To run Anomaly, changepoint, and trend detection:

$ python3 run.py score --kwargs


To rank and post the resulting timeseries to slack:

$ python3 run.py rank_and_plot --kwargs