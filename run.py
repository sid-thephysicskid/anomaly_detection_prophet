import sys, os

base_dir = os.path.join(os.getenv('HOME'))
data_dir = os.path.join(base_dir, "")
model_dir = os.path.join(base_dir, '')

sys.path.append(os.path.join(base_dir, ""))


import argparse
from anomaly.data_utils import QueryMetadata, SQLDataGenerator, create_sql_engine
from anomaly.anomaly_utils import ProphetAnomalyDetector
from anomaly.plot import prophet_plot




queries = [
    QueryMetadata("driver count", '''
    SELECT a.customer as customer,
    DATE(a.startdate) as ds,
    COUNT(DISTINCT r.driverid) as y
    FROM log_riskscore r
    LEFT JOIN log_apicall a on r.apicallid=a.id
    AND a.startdate > date :start_date
    AND a.startdate <= date :end_date
    WHERE a.route ='score_drivers'
    GROUP BY a.customer, DATE(a.startdate)
    '''),
    
    QueryMetadata("speeding", '''
    select customer, DATE(e.date) as ds, count(*) as y
    from drivers d, telematicsalerts e
    where e.driverid=d.id
    and telematicstype='speed'
    AND e.date >= date :start_date
    AND e.date < date :end_date
    group by customer, DATE(e.date)
    order by customer, DATE(e.date)
    ''', interpolate=True),
    
    QueryMetadata("highway observations", '''
    select customer, DATE(e.date) as ds, count(*) as y
    from drivers d, highwayobservations e
    where e.driverid=d.id
    AND e.date >= date :start_date
    AND e.date < date :end_date
    group by customer, DATE(e.date)
    order by customer, DATE(e.date)
    ''')
]

#generate data
sql_engine = create_sql_engine()
def generate_data(queries, engine, extract_window=60):      
    data_generators = [SQLDataGenerator(engine=engine, query_metadata=queries, extract_window=extract_window)]
    raw_data = []
    for dg in data_generators:
        results = dg.run()
        raw_data.extend(results)
    return raw_data


#parse arguments to anomaly detector (Prophet)
parser = argparse.ArgumentParser()
parser.add_argument('-tst','--test_window', type=int, metavar='', default=7, help='number of test days for detecting anomalies')
parser.add_argument('-trn','--train_window', type=int, metavar='', default=14, help='number of train days; recommended to be more than test window')
parser.add_argument('-piw','--prophet_interval_width', type=float, metavar='', default=0.95, help='width x means 100*x percent of samples should fit between yhat_upper and yhat_lower')
parser.add_argument('-pcp','--prophet_changepnt_prior', type=float, metavar='', default=0.15, help='higher value means more flexible trend')
args = parser.parse_args()


prophet_kwargs = dict(test_window=args.test_window, train_window=args.train_window, beta=0.1, 
                                     ruptures_changepnt_penalty=10, prophet_interval_width=args.prophet_interval_width, 
                                     prophet_changepnt_prior = args.prophet_changepnt_prior, weekly_seasonality= False)

def main():
    raw_data = generate_data(queries, sql_engine)
    anomaly_detectors = [ProphetAnomalyDetector(anomaly_data, **prophet_kwargs) for anomaly_data in raw_data]
    results = [detector.apply() for detector in anomaly_detectors]
    results = sorted(results, key=lambda detector: detector.data.P, reverse=False)
    for ad in results:
        if ad.data.P >0:
            prophet_plot(ad, post_to_slack=True)

if __name__=="__main__":
    main()
