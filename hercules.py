# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 10:51:50 2023
A new version of the hercules dashboard that uses sqlite3 to store data
@author: David.Kenward
"""


import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import re
import sqlite3
import numpy as np

st.set_page_config(page_title='Hercules Dashboard', page_icon=":chart_with_upwards_trend:",
                    layout='wide'
)

#a sorting function for later
def sorted_alphanumeric(data):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(data, key=alphanum_key)

#The first time the program runs, we want to create a folder with an
#initialization file where we can store the default filepath to
#where the data lives.  On subsequent runs, we want to read the ini file
init_file = Path("C:\MKS\Herc_dash\ini.txt")
if init_file.is_file():
    pathtxt = Path(init_file).read_text()
else:
    if not os.path.exists(Path('C:\MKS\Herc_dash')):
        os.makedirs(Path('C:\MKS\Herc_dash'))
    with open(os.path.join("C:\MKS\Herc_dash", "ini.txt"), "w") as ini_file:
        ini_file.write('')
    pathtxt = ''
    
#pathtxt = 'C:\\Users\\David.Kenward\\python sandbox\\test_data\\herc'
txtpath = st.text_input('Path to data', pathtxt)
#st.button("Set Directory")

if st.button("Click here to process new data"):
    #connect to database, search default folder for new data
    with st.spinner('Initializing Dashboard...'):        
        #open the mongodb client
        #client = MongoClient()
        #connect to the hercules db
        #db = client.hercules
        #use the data collection for labview and keysight data
        #data = db.data
        con = sqlite3.connect("hercules.db")
        cur = con.cursor()
            
        #use the manifest collection to keep track of processed files
        #manifest = db.manifest
        try:
            #manifest_df = pd.DataFrame(list(manifest.find()))
            manifest_df = pd.read_sql_query("SELECT * FROM manifest", con)
        except:
            manifest_df = pd.DataFrame(columns=['Processed_dates_lv', 'Processed_dates_ks'])
         
        #search the labview and keysight directories for files
        location = Path(txtpath)
        labview_files = os.path.join(location, 'LabVIEW')
        keysight_files = os.path.join(location, 'Keysight')
        
        #These are the directories with the relevant data
        #note, must be in mm-dd-yyy name format
        labview_dates = os.listdir(labview_files)
        keysight_dates = os.listdir(keysight_files)
        
        #remove the directories that have already been processed.
        #if the manifest is empty, we process all the dates
        if not manifest_df.empty:
            labview_dates = [labview_dates.remove(d) for d in labview_dates if d in manifest_df.Processed_dates_lv]
            keysight_dates = [keysight_dates.remove(d) for d in keysight_dates if d in manifest_df.Processed_dates_ks]
                
        for l, k in zip(labview_dates, keysight_dates):
            lv_dict = {}
            ks_dict = {}
            
            lv_data_files = sorted_alphanumeric(os.listdir(os.path.join(labview_files, l)))
            lv_dict[l] = [f for f in lv_data_files if f.endswith('.csv')]
            
            ks_data_files = sorted_alphanumeric(os.listdir(os.path.join(keysight_files, k)))
            ks_dict[k] = [f for f in ks_data_files if f.endswith('.csv')]
        
        #keep track of errors we could not handle
        lv_file_errors = []
        lv_unable = []
        lv_handled_error_dict = {}
        lv_unhandled_error_dict = {}
        ks_file_errors = []
        ks_unhandled_errors = {}
        
        #the dicts contain the relevant data files and the key vals are the dates the
        #data were started.  The keys should be the same for each dict, do a quick check here
        labview_keys = list(lv_dict)
        keysight_keys = list(ks_dict)
        lv_not_ks = [l for l in labview_keys if l not in keysight_keys]
        ks_not_lv = [l for l in keysight_keys if l not in labview_keys]
        
        #keys is the list of keys which are in both dicts
        keys = list(set(labview_keys) & set(keysight_keys))
        
        #loop through each date key
        for key in keys:
            #the labview and keysight file directories for the dates
            lv_date_directory = os.path.join(labview_files, key)
            ks_date_directory = os.path.join(keysight_files, key)
            lv_filepaths = []
            ks_filepaths = []
            
            #get the list of files for lv and ks for each date
            for val in lv_dict[key]:
                lv_filepaths.append(os.path.join(lv_date_directory, val))
            for val in ks_dict[key]:
                ks_filepaths.append(os.path.join(ks_date_directory, val))
            
            #remove any file that isn't a .csv file
            lv_filepaths = [l for l in lv_filepaths if l.endswith('.csv')]
            ks_filepaths = [l for l in ks_filepaths if l.endswith('.csv')]
            
            lvdf = pd.read_csv(lv_filepaths.pop(0), skiprows=2)
            header = list(lvdf)
            #build the labview dataframe
            for lfp in lv_filepaths:
                #check that the file is formatted the same
                try:
                    lvdf = pd.concat([lvdf, pd.read_csv(lfp, skiprows=3, names=header)], ignore_index=True, axis=0)
                except:
                    lv_file_errors.append(lfp)
                    try:
                        lvdf = pd.concat([lvdf,  pd.read_csv(lfp, on_bad_lines='skip', skipfooter=2, names=header)], ignore_index=True, axis=0)
                    except:
                        lv_unable.append(lfp)
            #remove empty column and rows were NaN
            lvdf = lvdf.drop('Unnamed: 26', axis=1)
            lvdf = lvdf.dropna()
            #remove lines  where the header appears again
            lvdf = lvdf[lvdf.th3 != 'th3']
            
            #convert the timestamp column to datetime obj
            start_time = list(lvdf)[0]
            #jeff says "it's seconds after the initial time with the first number being zero, so you can
            #subtract the first number from everything and then it will make sense"
            lvdf[start_time] = pd.to_numeric(lvdf[start_time], downcast='float')
            lvdf[start_time] = lvdf[start_time] - lvdf[start_time].iloc[0]
            #convert the start time from string to datetime
            dt_start = datetime.strptime(start_time, '%m/%d/%y %H:%M:%S.%f')
            #finally, convert the column to datetime objects
            lvdf[start_time] = dt_start + pd.to_timedelta(lvdf[start_time], unit='s')
            
            #build the keysight dataframe
            ksdf = pd.read_csv(ks_filepaths.pop(0))
            for kfp in ks_filepaths:
                try:
                    ksdf = pd.concat([ksdf, pd.read_csv(kfp)], ignore_index=True)
                except:
                    ks_file_errors.append(kfp)
            
            ksdf['Time'] = pd.to_datetime(ksdf['Time'])
            
            #now we need to join the data
            #rename labview timestamp column to have same name as keysight
            lvcols = list(lvdf)
            lvcols[0] = 'Time'
            lvdf.set_axis(lvcols, axis=1, inplace=True)
            
            #perform the merge
            merged = pd.merge_asof(lvdf, ksdf, on='Time', direction='nearest')
    
            #convert all non datetime columns to float
            merged_cols = merged.columns
            merged[merged_cols[1:]] = merged[merged_cols[1:]].apply(pd.to_numeric, errors='coerce')
            
            #save the data to the database
            merged.to_sql(name='Data', con=con, if_exists='append')
            #data_update = data.insert_one(merged.to_dict())
            
            
            add_manifest_date = {'Processed_dates_lv':key,
                                 'Processed_dates_ks':key}
            manifest_update = pd.DataFrame(data=[[key, key]], columns=['Processed_dates_lv','Processed_dates_ks'])
            manifest_update.to_sql(name='manifest', con=con, if_exists='append')
            #manifest_update = manifest.insert_one(add_manifest_date)
            
            if lv_unable:
                with st.warning("WARNING! The following labview files were unable to be processed:"):
                    for i in lv_unable:
                        st.markdown("- " + i)
            if ks_file_errors:
                with st.warning("WARNING! The following keysight files were unable to be processed:"):
                    for i in ks_file_errors:
                        st.markdown("- " + i)
  
con = sqlite3.connect("hercules.db")

@st.cache
def load_data(con):
    df = pd.read_sql_query("SELECT * FROM Data", con)
    df['Time'] = pd.to_datetime(df['Time'].str.strip(), infer_datetime_format=True)
    return df
df = load_data(con)
with st.expander('Click to view merged data', expanded=False):
    st.dataframe(df)
    
with st.expander('Click to show plot', expanded=False):
    lines = st.multiselect('Lines to plot', list(df))
        
    fig = px.scatter(df, x='Time', y='fet_temp')
    
    for l in lines:
        fig = fig.add_scatter(x=df['Time'], y=df[l])
    
    st.plotly_chart(fig, use_container_width=True)
    
stat_cols = st.multiselect("Select columns to view min/max/mean/delta/std", list(df),
                           default=['th1', 'th2', 'th3', 'th4'])

#create the date selection
dates = df.Time.dt.date
first_date = dates.iloc[0]
last_date = dates.iloc[-1]
start_date = st.date_input("Select start date", value=first_date, min_value=first_date, max_value=last_date)
end_date_min = start_date + timedelta(days=1)
end_date = st.date_input("Select end date", value=last_date, min_value=end_date_min, max_value=last_date)

#convert end and start dates to datetime64 type for comparison
sd64 = np.datetime64(start_date)
ed64 = np.datetime64(end_date)

#filter the dataframe by time
dff = df[(df.Time >= sd64) & (df.Time <= ed64)]

avg_vals_by_port = dff.groupby('analog port').mean(numeric_only=True)
max_vals_by_port = dff.groupby('analog port').max(numeric_only=True)
min_vals_by_port = dff.groupby('analog port').min(numeric_only=True)
std_by_port = df.groupby('analog port').std(numeric_only=True)
delta_by_port = max_vals_by_port - min_vals_by_port

with st.expander("Average values"):
    st.table(avg_vals_by_port[stat_cols])
with st.expander("Maximum values"):
    st.table(max_vals_by_port[stat_cols])
with st.expander("Minimum values"):
    st.table(min_vals_by_port[stat_cols])
with st.expander("Delta values"):
    st.table(delta_by_port[stat_cols])
with st.expander("Standard Deviation"):
    st.table(std_by_port[stat_cols])
    
group_lines = st.multiselect("Select columns to view grouped by analog port", list(df),
                             default=['th1', 'th2', 'th3', 'th4'])
figg = px.scatter(dff, x='Time', y=group_lines, facet_row='analog port')
figg.update_layout(
    height=1000,
    yaxis=dict(
        title_text="Temperature"),
    yaxis2=dict(
        title_text='Temperature'),
    yaxis3=dict(
        title_text='Temperature'),
    yaxis4=dict(
        title_text='Temperature'))
st.plotly_chart(figg, use_container_width=True)