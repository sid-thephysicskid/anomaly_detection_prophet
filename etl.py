# Load and rename columns etl.py
import pandas as pd
#Take session and write a data generator class to fetch a list of dbs, write a dataclass
def load_df():
    #Need help here
    pass

def rename_columns_for_prophet(df_inp, old_column_list):

    #Need help here too, a simple renaming as written below could work as long
    #we ensure that input df comes with only 2 columns, first being datetime
    #and the next being counts or whatever the anomaly is supposed to run on
    df = df_inp[old_column_list]
    new_column_list = ['ds','y']
    df = df.rename(columns=dict(zip(old_column_list, new_column_list))) 
    df.ds = pd.to_datetime(df.ds)
    return df 

def reindex_df(df_inp):
    '''
    '''
    start_date = df_inp.ds.iloc[0] 
    end_date = df_inp.ds.iloc[-1]
    idx = pd.date_range(start_date, end_date)

    df = df_inp.set_index('ds')#,inplace=True)
    df = df.reindex(idx)
    df['y']= df['y'].interpolate(method='linear')
    df = df.rename_axis('ds').reset_index()
    return df

