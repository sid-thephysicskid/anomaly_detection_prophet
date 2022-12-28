import pandas as pd
# import prophet
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
# import plotly.express as px
# from prophet import Prophet
import ruptures as rpt
from ruptures.utils import pairwise
from itertools import cycle
# import seaborn as sns

mpl.rcParams['figure.figsize'] = (20, 16)
mpl.rcParams['axes.grid'] = False


def apply_changepnt_detection(df,changepoint_model="rbf", changepoint_penalty=10):
    '''
    Apply changepoint detection ala ruptures package and generate breakpoint indices
    Args
    ---
    changepoint_model: str
        Pelt segment model, ["l1", "l2", "rbf"] 
        https://centre-borelli.github.io/ruptures-docs/code-reference/detection/pelt-reference/
    changepoint_penalty: float
        Higher the value, more conservative the segmentation, must be >0
    
    Returns
    ---
    breakpoints: list
        List of breakpoint indices
    '''
    signal = df['y'].to_numpy()
    algo = rpt.Pelt(model=changepoint_model).fit(signal)
    result = algo.predict(pen=changepoint_penalty)
    breakpoints = [0] + sorted(result)
    return breakpoints

#Plot changepoints
def plot_changepoints(df_inp, changepoints=list(),COLOR_CYCLE = ["#4286f4", "#f44174"]):    
    color_cycle = cycle(COLOR_CYCLE)
    fig, ax = plt.subplots(1,1,figsize=(16,8))
    ax.plot(df['ds'],df['y'], '-o', markersize=2, linewidth=1)
    for (start, end), col in zip(pairwise(changepoints), color_cycle):
        ax.axvspan(df.iloc[max(0,start-1)]['ds'], df.iloc[end-1]['ds'], facecolor=col,alpha=0.2)
    plt.show()

# 2) Fit prophet 

def prophet_fit(df, prophet_model, changepoints_list, test_window = 7, train_window = 14):
    """
    Fit the model to the time-series data and generate forecast for specified time frames
    Args
    ----
    df : pandas DataFrame
        The daily time-series data set contains ds column for
        dates and y column for numerical values
    prophet_model : Prophet model
        Prophet model with configured parameters
    changepoints_list : list
        List of breakpoint indices as evaluated by ruptures package
    test_window : int
        Number of days for Prophet to make predictions for
    train_window: int
        Min number of days leading upto the start of the test window

    Returns
    -------
    forecast : pandas DataFrame
        The predicted result in a format of dataframe
    prophet_model : Prophet model
        Trained model
    """

    # segment the time frames
    end_index = df.index[-1]
    test_start_index = end_index - test_window + 1 
    print(f'TEST start index is {test_start_index}')
    print(f'TEST END index is {end_index}') #the test end index is setup to be the last index of df
    if test_start_index - changepoints_list[-2] < train_window:
        train_start_index = test_start_index - train_window
    else:
        train_start_index = changepoints_list[-2]
    print(f'TRAIN start index is {train_start_index}')

    train_end_index = test_start_index - 1
    print(f'TRAIN end index is {train_end_index}')
    baseline_ts = df['ds'][train_start_index:train_end_index]
    baseline_y = df['y'][train_start_index:train_end_index]
    
    print('TRAIN from {} to {}'.format(df['ds'][train_start_index], df['ds'][train_end_index]))
    print('PREDICT from {} to {}'.format(df['ds'][test_start_index], df['ds'][end_index]))

    # fit the model
    prophet_model.fit(pd.DataFrame({'ds': baseline_ts.values,
                                    'y': baseline_y.values}),  algorithm = 'Newton')
    
    future = prophet_model.make_future_dataframe(periods=test_window)
    # make prediction
    forecast = prophet_model.predict(future)
    return forecast, prophet_model


def get_outliers(df, forecast, beta=0.1, test_window=7):
    """
    Combine the actual values and forecast in a data frame and identify the outliers
    Args
    ----
    df : pandas DataFrame
        The daily time-series data set contains ds column for
        dates (datetime types such as datetime64[ns]) and y column for numerical values
    forecast : pandas DataFrame
        The predicted result in a dataframe which was previously generated by
        Prophet's model.predict(future)
    test_window : int
        Number of days for Prophet to make predictions for
    Returns
    -------
    outliers : a list of (datetime, int, int) triple
        A list of outliers, the date, the value, and penalty for each
    df_pred : pandas DataFrame
        The data set contains actual and predictions for the forecast time frame
    P : int
        Net penalty value of all the outliers detected
    """
    df_pred = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(test_window)
    df_pred.index = df_pred['ds'].dt.to_pydatetime()
    df_pred.columns = ['ds', 'preds', 'lower_y', 'upper_y']
    end_index = df.index[-1]

    test_start_index = end_index - test_window 
    df_pred['actual'] = df['y'][test_start_index: end_index].values

    # construct a list of outliers
    outlier_index = list()
    outliers = list()
    penalty = list()
    P = 0 # net penalty
    for i in range(df_pred.shape[0]):
        actual_value = df_pred['actual'][i]
        pred_value   = df_pred['preds'][i]
        lower_bound  = (1-beta)*df_pred['lower_y'][i]
        upper_bound  = (1+beta)*df_pred['upper_y'][i]
        if actual_value < lower_bound:
            outlier_index += [i]
            p = (pred_value - actual_value)/pred_value
            penalty.append(p)
            outliers.append((df_pred.index[i], actual_value, p))
            
        elif actual_value > upper_bound:
            outlier_index += [i]
            p = (actual_value - pred_value)/pred_value
            penalty.append(p)
            outliers.append((df_pred.index[i], actual_value, p))            

            # print out the evaluation for each outlier
            print('=====')
            print('actual value {} fall outside of the prediction interval'.format(actual_value))
            print('interval: {} to {}'.format(df_pred['lower_y'][i], df_pred['upper_y'][i]))
            print('Date: {}'.format(str(df_pred.index[i])[:10]))

    P = sum(penalty)
    print('Net Penalty for the prediction interval of last {} days is {}'.format(test_window, P))
    for outlier in outliers:
        print(outlier)
    return outliers, df_pred, P


def prophet_plot(df, forecast, prophet_model, changepoints_list, outliers=list()):
    """
    Plot the actual, predictions, and anomalous values
    Args
    ----
    df : pandas DataFrame
        The daily time-series data set contains ds column for
        dates (datetime types such as datetime64[ns]) and y column for numerical values

    outliers : a list of (datetime, int) tuple
        The outliers we want to highlight on the plot.
    """
    # generate the plot
    fig = prophet_model.plot(forecast)
    
    # retrieve the subplot in the generated Prophets matplotlib figure
    ax = fig.get_axes()[0]

    #plot actual values
    x_pydatetime = df['ds'].dt.to_pydatetime()
    ax.plot(x_pydatetime,
        df.y,
        color='orange', label='Actual')

    # plot each outlier in red, uncomment the second line to annotate date (makes it super crowded though)
    for outlier in outliers:
        ax.scatter(outlier[0], outlier[1], s = 16, marker='x', color='red', label='Anomaly')
        # ax.text(outlier[0], outlier[1], str(outlier[0])[:10], color='red')


    # re-organize the legend
    patch1 = mpatches.Patch(color='red', label='Anomaly')
    patch2 = mpatches.Patch(color='orange', label='Actual')
    patch3 = mpatches.Patch(color='skyblue', label='Prediction interval')
    plt.legend(handles=[patch1, 
                        patch2, 
                        patch3, 
                        ])
    
    #plot the changepoints
    COLOR_CYCLE = ["#4286f4", "#f44174"]
    color_cycle = cycle(COLOR_CYCLE)
    for (start, end), col in zip(pairwise(changepoints_list), color_cycle):
        ax.axvspan(df.iloc[max(0,start-1)]['ds'], df.iloc[end-1]['ds'], facecolor=col,alpha=0.2)

    plt.show()