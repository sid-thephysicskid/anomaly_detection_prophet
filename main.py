
# from utils import apply_changepnt_detection, prophet_fit, prophet_plot, get_outliers, plot_changepoints

from refactor_utils import AnomalyDetector
from etl import rename_columns_for_prophet, reindex_df
import pandas as pd
from pathlib import Path

import argparse

# construct the argument parser and parse the arguments
# ap = argparse.ArgumentParser()
# ap.add_argument("-a", "--alpha", default=0.95, type=float,
# 	help="Uncertainty interval width for Prophet predictions; larger equals more uncertainty")
# ap.add_argument("-c", "--changepoint_prior_scale", default=0.15, type=float,
# 	help="measure of trend's flexibility, larger values allow more flexibility")
# ap.add_argument("-test", "--test_window", default=14, type=int,
#     help="number of data points to forecast from the latest one  ")
# ap.add_argument("-train", "--train_window", default=21, type=int,
#     help="number of time series data points to train" )
# ap.add_argument("-p","--plot",required=True,
# 	help="path to output the loss plot, includes file name")
# args = vars(ap.parse_args())

data_dir = Path('./data')

df_raw = pd.read_csv(data_dir/'tforce_driver_scored.csv')
df = rename_columns_for_prophet(df_raw, ['startdate','ndriver_distinct'])
df = reindex_df(df)

def main():
    foo = AnomalyDetector(df)
    foo.apply_changepnt_detection().prophet_fit().get_outliers().prophet_plot()
    print(foo.P)
    exit()

if __name__ == "__main__":
    main()


#old code


# alpha=0.95
# m = Prophet(interval_width=alpha,
#             yearly_seasonality=False,
#             weekly_seasonality=False,
#             changepoint_prior_scale=0.15
#             )
# # model.add_seasonality(name='weekly', period=7, fourier_order=3, prior_scale=0.3)

# chngpnts = apply_changepnt_detection(df, changepoint_penalty=10)
# forecast, model = prophet_fit(df, m, chngpnts, train_window = 42, test_window = 30)
# outliers, df_pred, penalty = get_outliers(df, forecast, beta=0.05, test_window=30)
# prophet_plot(df, forecast, model, chngpnts, outliers=outliers)