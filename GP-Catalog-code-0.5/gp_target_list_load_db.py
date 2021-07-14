import os
import time
import sys, stat
import math
import shutil
from os.path import exists, join
import os.path
from os import pathsep
import string
import os
import io
import sys
import json
import datetime
import time
import numpy as np
# from sqlalchemy import create_engine
# import psycopg2
# import psycopg2.extras
import pandas as pd
from do_write import write_main

from math import nan, isnan

def gp_catalogue_table(gp_catalogue_file):
    df = pd.read_csv(gp_catalogue_file,header=None,names=
        ['onboard_time', 'gsp_s', 'gsp_e', 'duration', 'OBS_TYPE', 
        'OBS_ID_NUMBER', 'SVOM_OBSDB_ID', 'RIGHT_ASCENSION', 'DECLINATION', 'qc', 
        'q1', 'q2', 'q3', 'comments', 'MXT_CONF', 'ECL_CONF', 
        'VT_EXPOSURE_TIME', 'VT_WINDOW_SIZE', 'VT_INTERVAL_BETWEEN_IMG', 'VT_READ_SPEED', 'VT_READ_CHANNEL', 'VT_CLEANING',
        'REQUESTED_OBS_DURATION_IN_MINUTES', 'angz', 'angy', 'angx',  'sun_angle',  'moon_angle', 
         'anglesun_yp',  'anglesun_ym', 'anglesun_zm',  'sun_angle_onboard',  'moon_angle_onboard', 
         'anglesun_yp_onboard',  'anglesun_ym_onboard', 'anglesun_zm_onboard', 
         'PF_MOON_CHECK', 'PF_STABILITY','ATTRIBUTE', 'PRIORITY_LEVEL','COMBINED_OBSERVATION'
        ], sep=',', engine='python', skiprows=1)

# , 'TIME_CONSTRAINTS', 'TIME_CONSTRAINTS_START_DATE', 'TIME_CONSTRAINTS_END_DATE', 
#          'COMPLETENESS', 'ATTRIBUTE', 'PRIORITY_LEVEL', 'USER_GROUP', 'COMBINED_OBSERVATION', 'COMBINED_OBSERVATION_TEL'
    return(df)


pathsrc_slash = './'
gp_catalogue = 'gp_catalogue.csv'

df = gp_catalogue_table(pathsrc_slash+gp_catalogue)
# print(df)

df1 = df.fillna('nan')
datalist = df1.values.tolist()
print(datalist)

catalog_id = write_main(datalist)
print('Catalog ID:' + str(catalog_id))

# db = con_db()
# for i in range(len(df)):
#     i = i + 1
#     data_lines_input = [df['OBS_TYPE'].iloc[i],df['SVOM_OBSDB_ID'].iloc[i],df['RIGHT_ASCENSION'].iloc[i],df['DECLINATION'].iloc[i]
#         ,df['REQUESTED_OBS_DURATION_IN_MINUTES'].iloc[i],df['ECL_CONF'].iloc[i],df['MXT_CONF'].iloc[i],df['VT_EXPOSURE_TIME'].iloc[i]
#         ,df['VT_WINDOW_SIZE'].iloc[i],df['VT_INTERVAL_BETWEEN_IMG'].iloc[i],df['VT_READ_SPEED'].iloc[i],df['VT_READ_CHANNEL'].iloc[i]
#         ,df['VT_CLEANING'].iloc[i],df['PF_STABILITY'].iloc[i],df['PF_MOON_CHECK'].iloc[i],df['ATTRIBUTE'].iloc[i]
#         ,df['PRIORITY_LEVEL'].iloc[i],df['COMBINED_OBSERVATION'].iloc[i]]
#     print(list(df.iloc[i]))
#     # print(text)
#     catalog_id = write_main([list(df.iloc[i])])
#     print('Catalog ID:' + str(catalog_id))
# close_db(db)


