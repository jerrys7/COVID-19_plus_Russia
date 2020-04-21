from os.path import join, abspath
from os import walk
from datetime import datetime, date, timedelta
import pandas as pd
from dateutil import parser
from pandas import DataFrame, read_csv
from typing import Dict, List, NamedTuple, Tuple, Optional, Any
from collections import defaultdict
from copy import copy

from .defs import COVID19RU_ROOT,COVID19RU_TSROOT
from .check import filedate, is_format1, is_format2, is_format2_buggy


def load_format1(filepath:str)->DataFrame:
  pd1=read_csv(filepath)
  return DataFrame({
    'FIPS':None,
    'Admin2':None,
    'Province_State':pd1['Province/State'],
    'Country_Region':pd1['Country/Region'],
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

def load(root:str=COVID19RU_ROOT,
         province_state:Optional[str]=None,
         country_region:Optional[str]=None)->Dict[datetime, DataFrame]:
  """ Loads the dataset as a Dict mapping a datetime into DataFrame containing
  all the countries/states """
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
        if province_state is not None:
          pd=pd[pd['Province_State']==province_state]
        if country_region is not None:
          pd=pd[pd['Country_Region']==country_region]
        pds[date]=pd
  return pds

TimeLine=NamedTuple('TimeLine',[('dates',List[datetime]),
                                ('confirmed',List[int]),
                                ('deaths',List[int]),
                                ('recovered',List[int]),
                                ('daily_cases',List[int]),
                                ('daily_cases_ma7',List[float])])

def daily_cases(l:List[int])->List[int]:
  res=[]
  for i in range(len(l)):
    res.append(l[i]-l[i-1] if i>0 else 0)
  assert len(l)==len(res)
  return res

def ma7(l:List[int])->List[float]:
  res=[]
  ma=float(0.0)
  for i in range(len(l)):
    ma=ma*(6.0/7.0) + l[i]*(1.0/7.0)
    res.append(ma)
  assert len(l)==len(res)
  return res

def mktimeline(dates,confirmed,deaths,recovered)->TimeLine:
  d=daily_cases(confirmed)
  m=ma7(d)
  return TimeLine(dates,confirmed,deaths,recovered,d,m)

Province_State=str  # City or region name
Country_Region=str  # Country name

def timelines(province_state:Optional[str]=None,
              country_region:Optional[str]=None,
              default_loc:Optional[str]=None)->Dict[Tuple[Province_State,Country_Region],TimeLine]:
  assert province_state is not None or country_region is not None
  dates=defaultdict(list)
  confirmed=defaultdict(list)
  deaths=defaultdict(list)
  recovered=defaultdict(list)
  keys=[]
  for d,df in load().items():
    if province_state is not None:
      df=df[df['Province_State']==province_state]
    if country_region is not None:
      df=df[df['Country_Region']==country_region]
    if len(df.index)==0:
      continue
    def _fixnan(df, default=None):
      return df.where(pd.notnull(df), default)
    for i in range(len(df.index)):
      ps=_fixnan(df['Province_State'],default_loc).iloc[i]
      cr=_fixnan(df['Country_Region'],default_loc).iloc[i]
      dates[(ps,cr)].append(d)
      confirmed[(ps,cr)].append(_fixnan(df['Confirmed'],0).astype('int32').iloc[i])
      deaths[(ps,cr)].append(_fixnan(df['Deaths'],0).astype('int32').iloc[i])
      recovered[(ps,cr)].append(_fixnan(df['Recovered'],0).astype('int32').iloc[i])
      keys.append((ps,cr))
  ret={}

  for k in keys:
    ret[k]=mktimeline(dates[k],confirmed[k],deaths[k],recovered[k])
  return ret

def ru_timeline_regions(ds)->List[str]:
  df=ds[max(ds.keys())]['Province_State']
  return list(df[pd.notnull(df)])

def ru_timeline_dates(ds)->List[datetime]:
  dts=[]
  maxdate=max(ds.keys())
  d=datetime(2020,1,22)
  while d<=maxdate:
    dts.append(copy(d))
    d+=timedelta(days=1)
  return dts

def ru_timeline_get(ds, d:datetime, region:str, field:str='Confirmed', default:Any=0)->Any:
  df=ds.get(d)
  if df is None:
    return default
  df=df[df['Province_State']==region]
  if len(df.index)==0:
    return default
  if field not in df:
    return default
  df=df.where(pd.notnull(df), default)
  return df[field].iloc[0]

def ru_timeline_dump_(filename:str=join(COVID19RU_TSROOT,'time_series_covid19_confirmed_RU.csv'),
                      report_field:str='Confirmed')->None:
  ds=load(country_region='Russia')
  maxdate=max(ds.keys())
  headers="UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key".split(',')
  dates=ru_timeline_dates(ds)
  with open(filename,'w') as f:
    f.write(','.join(headers+[d.strftime('%m/%d/%y') for d in dates])); f.write('\n')
    for r in ru_timeline_regions(ds):
      line:List[Any]=[]
      for h in headers:
        line.append(str(ru_timeline_get(ds,maxdate,r,h,'')))
      for d in dates:
        line.append(str(ru_timeline_get(ds,d,r,report_field,0)))

      line=[(f"\"{i}\"" if isinstance(i,str) and ',' in i else i) for i in line]
      f.write(','.join(line)); f.write('\n')

def ru_timeline_dump(tsroot:str=COVID19RU_TSROOT)->None:
  ru_timeline_dump_(join(tsroot, 'time_series_covid19_confirmed_RU.csv'),'Confirmed')
  ru_timeline_dump_(join(tsroot, 'time_series_covid19_deaths_RU.csv'),'Deaths')
