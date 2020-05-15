import json
import logging
import re
import urllib.request as request

from os.path import isdir, isfile, join, basename, splitext
from json import dump as json_dump, load as json_load
from typing import Optional, List, Dict, Any, NamedTuple, Tuple
from datetime import datetime
from time import sleep

from covid19ru.defs import ( COVID19RU_PENDING, REGIONS, LOCATION, LOCATION_DEF,
    CSSE2_HEADER )

RE_HTML = re.compile(r'class="config-view">(.+?)<')
RE_TIME = re.compile(r', (.+?) \(')

PendingData=NamedTuple('PendingData', [('utcnow',datetime),('val',dict)])


TIME="%d-%m-%Y__%H:%M:%S:%f"

def timestring(dt:Optional[datetime]=None)->str:
  """ Return timestamp in UTC """
  dt2= datetime.utcnow() if dt is None else dt
  return dt2.strftime(TIME)


def fetch_yandex_text()->str:
  with request.urlopen('https://yandex.ru/web-maps/covid19') as response:
    return response.read().decode('utf-8')

def fetch_yandex(dump_folder:Optional[str]=COVID19RU_PENDING)->PendingData:
  """ Fetch COVID19 data from Yandex
  Based on https://github.com/AlexxIT/YandexCOVID/blob/master/custom_components/yandex_covid/sensor.py
  """
  text = fetch_yandex_text()

  m = RE_HTML.search(text)
  assert m is not None, "Yandex page doesn't contain 'covid-view' tag"
  data = json.loads(m[1])

  attrs = {
      p['name']: {
          'cases': p['cases'],
          'cured': p['cured'],
          'deaths': p['deaths'],
          'coordinates':list(p['coordinates']), # [Lon,Lat] !!!
          'histogram':p.get('histogram',[])
      } \
      for p in data['covidData']['items'] \
      if ('ru' in p) and (p['ru'] is True)
  }

  data = PendingData(datetime.utcnow(), attrs)

  if dump_folder is not None:
    assert isdir(dump_folder)
    filepath = join(dump_folder,timestring(data.utcnow)+'.json')
    with open(filepath,'w') as f:
      json_dump(data.val, f, indent=4, ensure_ascii=False)
    print(f'Saved {filepath}')
  return data

def pending_timestamp(filepath:str)->datetime:
  return datetime.strptime(splitext(basename(filepath))[0],TIME)

def fetch_pending(filepath:str, dump_folder:str=COVID19RU_PENDING)->PendingData:
  ts:datetime=pending_timestamp(filepath)
  with open(join(dump_folder,filepath),'r') as f:
    d=json_load(f)
  data=PendingData(ts,d)
  # print(data)
  # print(ts)
  return data


REGIONS_RU_EN={r_ru:r_en for r_en,r_ru in REGIONS}
REGIONS_EN_RU={r_en:r_ru for r_en,r_ru in REGIONS}

def yandex_unpack_coordinates(dat:dict, default)->Tuple[float,float]:
  c=dat.get('coordinates')
  if c is not None:
    return (c[1],c[0])
  return default

def format_csse2(data:PendingData,
                 dump_folder:Optional[str]=COVID19RU_PENDING,
                 assert_unknown:bool=True)->List[str]:
  """ Format the data in the new CCSE format.

  Example output:
  ,,Moscow,Russia,3/22/20 00:00,55.75222,37.61556,191,1,0,"Moscow, Russia"
  ,,Moscow,Russia,2020-03-24 10:50:00,55.75222,37.61556,262,1,9,"Moscow, Russia"
  """
  res = []
  misses = []
  for c_ru,dat in data.val.items():
    if (not assert_unknown) and (c_ru not in {ru:en for en,ru in REGIONS}):
      misses.append(c_ru)
      continue
    c_en={ru:en for en,ru in REGIONS}[c_ru]

    update_time = data.utcnow.strftime("%Y-%m-%d %H:%M:%S")
    loc_lat,loc_lon = LOCATION.get(c_en, yandex_unpack_coordinates(dat,LOCATION_DEF))
    kw = f"{c_en},Russia"
    active=int(dat['cases'])-int(dat['deaths'])-int(dat['cured'])
    res.append((
      f",,\"{c_en}\",Russia,{update_time},{loc_lat},{loc_lon},"
      f"{dat['cases']},{dat['deaths']},{dat['cured']},{active},\"{kw}\""))

  if dump_folder is not None:
    filepath = join(dump_folder,timestring(data.utcnow)+'.csv')
    with open(filepath,'w') as f:
      f.write('\n'.join([CSSE2_HEADER]+res))
    print(f'Saved {filepath}')
  if len(misses)>0:
    print(f'Missed locations: {misses}')
  return res

def dryrun()->None:
  format_csse2(fetch_yandex(dump_folder=None), dump_folder=None, assert_unknown=True)


def monitor()->None:
  while True:
    try:
      format_csse2(fetch_yandex(), assert_unknown=False)
    except KeyboardInterrupt:
      raise
    except Exception as e:
      print('Exception', e, 'ignoring')
    for i in range(60):
      print(f'{60-i}..',end='',flush=True)
      sleep(60)


