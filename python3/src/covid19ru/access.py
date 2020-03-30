from os.path import join, abspath
from os import walk
from datetime import datetime, date
import pandas as pd
from dateutil import parser
from pandas import DataFrame, read_csv
from typing import Dict, List, NamedTuple, Tuple, Optional

from .defs import COVID19RU_ROOT
from .check import filedate, is_format1, is_format2, is_format2_buggy


def load_format1(filepath:str)->DataFrame:
  pd1=read_csv(filepath)
  return DataFrame({
    'FIPS':None,
    'Admin2':None,
    'Province_State':pd1['Province/State'],
    'Contry_Region':pd1['Country/Region'],
    'Last_Update':pd1['Last Update'],
    'Lat':None,
    'Long_':None,
    'Confirmed':pd1['Confirmed'],
    'Deaths':pd1['Deaths'],
    'Recovered':pd1['Recovered'],
    'Active':pd1['Confirmed']-pd1['Deaths']-pd1['Recovered'],
    'Combined_Key':None,
  })

def load_format2_buggy(filepath:str)->DataFrame:
  """ FIXME: keep right order of keys (currently it is not preserved)  """
  pd1=read_csv(filepath)
  d={k:pd1[k] for k in pd1.keys() if k!='Active' and k!='Last_Update'}
  d.update({
    'Active':pd1['Confirmed']-pd1['Deaths']-pd1['Recovered'],
    'Last_Update':pd1['Last_Update'],
    })
  return DataFrame(d)

def load(root:str=COVID19RU_ROOT)->Dict[datetime, DataFrame]:
  pds={}
  for root, dirs, filenames in walk(abspath(root), topdown=True):
    for filename in sorted(filenames):
      if filename.endswith('csv'):
        filepath=abspath(join(root, filename))
        date=filedate(filepath)
        if is_format1(filepath):
          pd=load_format1(filepath)
        elif is_format2(filepath):
          if is_format2_buggy(filepath):
            pd=load_format2_buggy(filepath)
          else:
            pd=read_csv(filepath)
        else:
          raise ValueError(f'Unsupported CSV format for {filepath}')
        pds[date]=pd
  return pds

TimeLine=NamedTuple('TimeLine',[('dates',List[datetime]),
                                ('confirmed',List[int]),
                                ('deaths',List[int]),
                                ('recovered',List[int])])

def timeline(province_state:str,country_region:Optional[str]=None)->TimeLine:
  dates=[]
  confirmed=[]
  deaths=[]
  recovered=[]
  for d,df in load().items():
    df=df[df['Province_State']==province_state]
    if country_region is not None:
      df=[df['Country_Region']==country_region]
    if len(df.index)==0:
      continue
    assert len(df.index)==1
    dates.append(d)
    confirmed.append(df['Confirmed'].iloc[0])
    deaths.append(df['Deaths'].iloc[0])
    recovered.append(df['Recovered'].iloc[0])
  return TimeLine(dates,confirmed,deaths,recovered)






