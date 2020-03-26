import json
import logging
import re
import urllib.request as request

from os.path import isdir, isfile, join
from json import dump as json_dump
from typing import Optional, List, Dict, Any, NamedTuple
from datetime import datetime

from .defs import COVID19RU_PENDING

RE_HTML = re.compile(r'class="config-view">(.+?)<')
RE_TIME = re.compile(r', (.+?) \(')

PendingData=NamedTuple('PendingData', [('utcnow',int),('val',dict)])


def timestring(dt:Optional[datetime]=None)->str:
  """ Return timestamp in UTC """
  TIME="%d-%m-%Y__%H:%M:%S:%f%z"
  dt2= datetime.utcnow() if dt is None else dt
  return dt2.strftime(TIME)


def fetch_yandex(dump_folder:Optional[str]=COVID19RU_PENDING)->PendingData:
  """ Fetch COVID19 data from Yandex
  Based on https://github.com/AlexxIT/YandexCOVID/blob/master/custom_components/yandex_covid/sensor.py
  """
  with request.urlopen('https://yandex.ru/web-maps/covid19') as response:
    text = response.read().decode('utf-8')

  m = RE_HTML.search(text)
  data = json.loads(m[1])

  attrs = {
      p['name']: {
          'cases': p['cases'],
          'cured': p['cured'],
          'deaths': p['deaths']
      }
      for p in data['covidData']['items']
  }

  items = data['covidData']['stat']['items']
  attrs['Россия'] = {
      'cases': int(items[0]['value']),
      'new_cases': int(items[1]['value']),
      'cured': int(items[2]['value']),
      'deaths': int(items[3]['value'])
  }
  # print(attrs)
  m = re.search(r', (.+?) \(', data['covidData']['subtitle'])
  state = m[1]
  data = PendingData(datetime.utcnow(), attrs)
  # print(state)
  if dump_folder is not None:
    assert isdir(dump_folder)
    filepath = join(dump_folder,timestring(data.utcnow)+'.json')
    with open(filepath,'w') as f:
      json_dump(data.val, f, indent=4, ensure_ascii=False)
    print(f'Saved {filepath}')
  return data



CITIES=[('Moscow','Москва'),
        ('Saint Petersburg','Санкт-Петербург'),
        ('Moscow oblast','Московская область'),
        ('Samara oblast','Самарская область'),
        ("Saha republic","Республика Саха (Якутия)"),
        ("Sverdlov oblast","Свердловская область"),
        ("Kaliningrad oblast","Калининградская область"),
        ("Kirov oblast","Кировская область"),
        ("Novosibirsk oblast","Новосибирская область"),
        ("Krasnoyarskiy kray","Красноярский край"),
        ("Tambov oblast","Тамбовская область"),
        ("Lipetsk oblast","Липецкая область"),
        ("Tver oblast","Тверская область"),
        ("Habarovsk oblast","Хабаровский край"),
        ("Tumen oblast","Тюменская область"),
        ("Tula oblast","Тульская область"),
        ("Perm oblast","Пермский край"),
        ("Nizniy Novgorod oblast","Нижегородская область"),
        ("Krasnodarskiy kray","Краснодарский край"),
        ("Voronezh oblast","Воронежская область"),
        ("Kemerovo oblast","Кемеровская область"),
        ("Republic of Hakassia","Республика Хакасия"),
        ("Murmansk oblast","Мурманская область"),
        ("Komi republic","Республика Коми"),
        ("Kaluga oblast","Калужская область"),
        ("Ivanovo oblast","Ивановская область"),
        ("Zabaykalskiy kray","Забайкальский край"),
        ("Tomsk oblast","Томская область"),
        ("Arkhangelsk oblast","Архангельская область"),
        ("Ryazan oblast","Рязанская область"),
        ("Republic of Chuvashia","Чувашская Республика"),
        ("Ulianovsk oblast","Ульяновская область"),
        ("Yaroslavl oblast","Ярославская область"),
        ("Pensa oblast","Пензенская область"),
        ("Belgorod oblast","Белгородская область"),
        ("Hanty-Mansiysk autonomy","Ханты-Мансийский АО"),
        ("Leningradskaya oblast","Ленинградская область"),
        ("Orenburg oblast","Оренбургская область"),
        ("Saratov oblast","Саратовская область"),
        ("Republic of Tatarstan","Республика Татарстан"),
        ("Kurgan oblast","Курганская область"),
        ("Repulbic of Kabardino-Balkaria","Кабардино-Балкарская Республика"),
        ("Cheliabinsk oblast","Челябинская область"),
        ("Stavropolskiy kray","Ставропольский край"),
        ("Briansk oblast","Брянская область"),
        ("Republik of Udmurtia","Удмуртская Республика"),
        ("Novgorod oblast","Новгородская область"),
        ("Republic of Crimea","Республика Крым"),
        ("Republic of Bashkortostan","Республика Башкортостан"),
        ("Chechen republic","Чеченская Республика"),
        ("Primorskiy kray","Приморский край"),
        ("Volgograd oblast","Волгоградская область"),
        ("Orel oblast","Орловская область"),
        ("Pskov oblast","Псковская область"),
        ("Rostov oblast","Ростовская область")
        ]


LOCATION={
    'Moscow':(55.75222,37.61556),
    'Saint Petersburg':(59.93863,30.31413)
    }

LOCATION_DEF=(61.52401,105.31875600000001)

CSSE2_HEADER=('FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,'
              'Confirmed,Deaths,Recovered,Active,Combined_Key')

def format_csse2(data:PendingData, dump_folder:Optional[str]=COVID19RU_PENDING)->List[str]:
  """ Format the data in the new CCSE format.

  Example output:
  ,,Moscow,Russia,3/22/20 00:00,55.75222,37.61556,191,1,0,"Moscow, Russia"
  ,,Moscow,Russia,2020-03-24 10:50:00,55.75222,37.61556,262,1,9,"Moscow, Russia"
  """
  res = []
  for c_en,c in CITIES:
    update_time = data.utcnow.strftime("%Y-%m-%d %H:%M:%S")
    loc_lat,loc_lon = LOCATION.get(c_en, LOCATION_DEF)
    dat = data.val[c]
    kw = f"{c_en},Russia"
    res.append((
      f",,\"{c_en}\",Russia,{update_time},{loc_lat},{loc_lon},"
      f"{dat['cases']},{dat['deaths']},{dat['cured']},,\"{kw}\""))

  if dump_folder is not None:
    filepath = join(dump_folder,timestring(data.utcnow)+'.csv')
    with open(filepath,'w') as f:
      f.write('\n'.join([CSSE2_HEADER]+res))
    print(f'Saved {filepath}')
  return res




