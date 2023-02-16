import pandas as pd
from sqlalchemy import text
from dataclasses import dataclass
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import create_engine


def create_sql_engine(data_url=None):
    if data_url:
        url = data_url
    elif data_url is None:
        load_dotenv()
        dialect = os.getenv('DS_DB_DIALECT')
        user = os.getenv('DS_DB_USER')
        password = os.getenv('DS_DB_PASSWORD')
        hostname = os.getenv('DS_DB_HOSTNAME')
        port = os.getenv('DS_DB_PORT')
        database = os.getenv('DS_DB_DATABASE')
        url = "{dialect}://{user}:{password}@{hostname}:{port}/{database}".format(
                    dialect=dialect,
                    user=user,
                    password=password,
                    hostname=hostname,
                    port=port,
                    database=database
                )
    engine = sqlalchemy.create_engine(url)
    return engine


@dataclass
class QueryMetadata:
    """Definition of a query.
    Return columns are (customer, date, value) and must be named ('customer', 'ds', 'y')
    Rows must be ordered by date within a customer
    """
    
    name : str
    query : str
    
    interpolate : bool = True
    ad_params : dict = None
    penalty_multiplier : float = 1.0

@dataclass
class AnomalyData:
    """Data for a single time series.
    """
    
    # extracted data
    customer : str
    series_name : str
    df : pd.DataFrame # data suitable for prophet: columns are (date, value) and have to be named (ds and y)
    
    # computed results
    P : float # total anomaly score for this series
    outliers : list # datapoints that are labeled as anomaly
    changepoints : list #indices that mark a change in data mean/variance
    extract_date : pd.Timestamp #



class DataGenerator(object):
    def run(self):
        raise Exception("Not implemented on base class")

class DatadogDataGenerator(DataGenerator):
    pass

class AstronomerDataGenerator(DataGenerator):
    pass


class SQLDataGenerator(DataGenerator):
    def __init__(self, 
                 query_metadata : QueryMetadata, 
                 engine, 
                 extract_window : int = 90, 
                 min_length_to_keep : int = None):
        self.query_metadata = query_metadata
        self.extract_window = extract_window
        self.engine = engine
        self.min_length_to_keep = min_length_to_keep
        if self.min_length_to_keep is None:
            self.min_length_to_keep = int(self.extract_window * 0.75)

    def run(self):
        today = pd.Timestamp('today')
        
        with self.engine.connect() as con:
            # This data contains all customers
            output = []
            
            for query in self.query_metadata:
                sql = text(query.query)
                query_df = pd.read_sql_query(sql, con, params={'start_date':(today - pd.Timedelta(days=self.extract_window)).strftime("%Y-%m-%d"), 
                                                               'end_date':today.strftime("%Y-%m-%d")})

                customer_names = query_df.customer.unique()
                for cust in customer_names:
                    series_df = query_df.drop('customer', axis=1)[query_df.customer == cust]
                    series_df.ds = pd.to_datetime(series_df.ds)
                    series_df.reset_index(inplace=True, drop=True)
                    # the index of all these dfs needs to be reset because the train/test indices wont work otherwise
                    series_df = self._reindex_df(series_df, interpolate=query.interpolate)
                    
                    series_name = query.name
                    ad = AnomalyData(customer=cust, series_name=series_name, df=series_df, P=0, outliers=[], changepoints=[],
                                     extract_date=today)#, ad_params=query.ad_params)
                    
                    output.append(ad)

        filtered_output = [i for i in output if self.min_length_to_keep <= 0 or len(i.df) >= self.min_length_to_keep]

        return filtered_output
    
    def _reindex_df(self, df_inp, *, interpolate=True):
        """Force series to have values for every day in range, interpolating missing values."""
        start_date = df_inp.ds.iloc[0] 
        end_date = df_inp.ds.iloc[-1]
        idx = pd.date_range(start_date, end_date)

        df = df_inp.set_index('ds')#,inplace=True)
        df = df.reindex(idx)
        ## preserve the orig y
        df['y_orig'] = df['y']
        if interpolate and len(df_inp) > 2:
            df['y']= df['y'].interpolate(method='linear')
        else:
            df['y']= df['y'].fillna(0)
        df = df.rename_axis('ds').reset_index()
        return df


def save_raw(extracted_data, path = 'data/raw', filename = 'raw_df_list.pkl'):
    """Saves the extracted data to data folder."""
    print('saving the list of extracted dataframes')  
    pickle.dump(extracted_data, open(path + '/' + filename, 'wb'))



