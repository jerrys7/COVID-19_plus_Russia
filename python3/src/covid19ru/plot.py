import matplotlib.pyplot as plt
from math import pow
from typing import Dict, Optional, Tuple
from .access import load, timelines, TimeLine, Province_State, Country_Region
from .fetch import REGIONS_EN_RU
from itertools import chain
from datetime import datetime
from collections import OrderedDict

import locale
locale.setlocale(locale.LC_TIME, "en_US")

def timelines_preprocess()->Dict[Tuple[Province_State,Country_Region],TimeLine]:
  """ Merge Moscow and Moscow oblast """
  tls=timelines(country_region='Russia', default_loc='')

  def _todict(tl:TimeLine)->dict:
    return {date:(c,d,r) for date,c,d,r in zip(tl.dates,tl.confirmed,tl.deaths,tl.recovered)}

  tl_m=_todict(tls[('Moscow','Russia')])
  tl_mo=_todict(tls[('Moscow oblast','Russia')])

  tl=TimeLine([],[],[],[])
  for d in sorted(list(set(tl_m.keys()).union(set(tl_mo.keys())))):
    tl.dates.append(d)
    rm=tl_m.get(d,(0,0,0))
    rmo=tl_mo.get(d,(0,0,0))
    tl.confirmed.append(rm[0]+rmo[0])
    tl.deaths.append(rm[1]+rmo[1])
    tl.recovered.append(rm[2]+rmo[2])
  # print(tl)
  tls[('Moscow+MO','Russia')]=tl
  del tls[('Moscow','Russia')]
  del tls[('Moscow oblast','Russia')]
  return tls

def plot(confirmed_min_threshold=50, show:bool=False,
         save_name:Optional[str]=None, labels_in_russian:bool=False)->None:
  plt.figure(figsize=(16, 6))
  plt.yscale('log')

  max_tick=0
  min_confirmed=99999999
  tls=timelines_preprocess()
  out:Dict[Tuple[str,str],TimeLine]=OrderedDict()
  out.update({k:v for k,v in sorted(tls.items(), key=lambda i:-i[1].confirmed[-1]) })
  out.update(timelines(country_region='Italy', default_loc=''))
  out.update(timelines(country_region='Japan', default_loc=''))
  lastdate=out[('Moscow+MO','Russia')].dates[-1]
  ndays_in_russia_after_threshold=(lastdate-out[('Moscow+MO','Russia')].dates[0]).days

  for (ps,cr),tl in out.items():
    # print(ps,cr)
    if len(ps)==0 and cr=='Russia':
      continue # Skip whole Russia which is similar to Moscow
    if tl.confirmed[-1]<10:
      continue

    ticks=[]; tick=0; confirmed=[]
    for d,c in zip(tl.dates,tl.confirmed):
      if c<=confirmed_min_threshold:
        continue
      if tick>ndays_in_russia_after_threshold:
        continue
      ticks.append(tick)
      confirmed.append(c)
      tick+=1

    if len(confirmed)==0:
      continue
    max_tick=max(max_tick,tick)
    min_confirmed=min(min_confirmed,confirmed[0])

    if labels_in_russian:
      label={'Moscow+MO':'Москва+область'}.get(ps,REGIONS_EN_RU.get(ps))
      if label is None:
        label={'Russia':'Россия',
               'Italy':'Италия',
               'Japan':'Япония'}.get(cr,cr)
    else:
      label=ps or cr

    alpha=0.6 if cr in ['Italy','Japan'] else 1.0
    color={'Italy':'#d62728',
           'Japan':'#9467bd'}.get(cr)
    p=plt.plot(ticks, confirmed, label=label, alpha=alpha, color=color)

  def _growth_rate_label(x):
    if labels_in_russian:
      return f'Прирост {x}%'
    else:
      return f'{x}% growth rate'

  plt.plot(range(max_tick),[min_confirmed*pow(1.05,x) for x in range(max_tick)],
           color='grey', linestyle='--', label=_growth_rate_label(5), alpha=0.5)
  plt.plot(range(max_tick),[min_confirmed*pow(1.3,x) for x in range(max_tick)],
           color='grey', linestyle='--', label=_growth_rate_label(30), alpha=0.5)
  # plt.plot(range(max_tick),[min_confirmed*pow(1.85,x) for x in range(max_tick)],
  #          color='grey', linestyle='--', label=_growth_rate_label(85), alpha=0.5)

  if labels_in_russian:
    plt.title(f"Число подтвержденных случаев COVID19 в регионах России на {lastdate.strftime('%d.%m.%Y')}")
    plt.xlabel(f"Количество дней с момента {confirmed_min_threshold}-го подтвержденного случая")
    plt.ylabel("Подтвержденных случаев")
  else:
    plt.title(f"Confirmed COVID19 cases in regions of Russia, as of {lastdate.strftime('%d %B %Y')}")
    plt.xlabel(f"Number of days since {confirmed_min_threshold}th confirmed")
    plt.ylabel("Confirmed cases")

  from matplotlib.font_manager import FontProperties
  fontP = FontProperties()
  fontP.set_size('small')

  plt.grid(True)
  plt.legend(loc='upper right', prop=fontP)

  # handles, labels = plt.gca().get_legend_handles_labels()
  # # sort both labels and handles by labels
  # labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0]))
  # plt.gca().legend(handles, labels)

  if save_name is not None:
    plt.savefig(save_name)
    print(f'Saved to {save_name}')
  if show:
    plt.show()

if __name__ == '__main__':
  plot()
