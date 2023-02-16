import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from ruptures.utils import pairwise
from itertools import cycle
from slack.web.client import WebClient
from slack.errors import SlackApiError
from dotenv import load_dotenv
mpl.rcParams['figure.figsize'] = (20, 16)
mpl.rcParams['axes.grid'] = False

from slack.web.client import WebClient
load_dotenv()
client =WebClient(os.getenv('SLACK_TOKEN'))



def prophet_plot(ad, color_cycle = ["#4286f4", "#f44174"], post_to_slack=False):
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
    fig = ad.model.plot(ad.forecast)
    
    # retrieve the subplot in the generated Prophets matplotlib figure
    ax = fig.get_axes()[0]
    ax.set_ylim(ymin=0)
    #plot actual values
    x_pydatetime = ad.data.df['ds'].dt.to_pydatetime()
    ax.plot(x_pydatetime,
        ad.data.df.y,
        color='orange', label='Actual') 
    ax.set_title(f'{ad.data.customer}: {ad.data.series_name} -- penalty for last {ad.test_window} days is {ad.data.P}')

    # plot each outlier in red, uncomment the second line to annotate date (makes it super crowded though)
    for outlier in ad.data.outliers:
        ax.scatter(outlier[0], outlier[1], s = 30, marker='X', color='red', label='Anomaly')
        # ax.text(outlier[0], outlier[1], str(outlier[0])[:10], color='red')

    ax.scatter(x_pydatetime[-ad.test_window:],ad.data.df.y_orig[-ad.test_window:], s=30, marker='1', color='green')
    # re-organize the legend
    patch1 = mpatches.Patch(color='red', label='Anomaly')
    patch2 = mpatches.Patch(color='orange', label='Actual')
    patch3 = mpatches.Patch(color='skyblue', label='Prediction interval')
    plt.legend(handles=[patch1, 
                        patch2, 
                        patch3, 
                        ])
    
    #plot the changepoints

    color_cycle = cycle(color_cycle)
    for (start, end), col in zip(pairwise(ad.data.changepoints), color_cycle):
        ax.axvspan(ad.data.df.iloc[max(0,start-1)]['ds'], ad.data.df.iloc[end-1]['ds'], facecolor=col,alpha=0.2)

    if post_to_slack:
        plt.savefig('AD.jpg')
        try:
            # client = create_slack_client()
            response = client.files_upload(
                file='AD.jpg',
                title=f'{ad.data.customer} Penalty = {ad.data.P}',
                channels='anomaly-detection'
            )
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
    plt.close(fig)