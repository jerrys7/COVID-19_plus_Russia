import matplotlib.pyplot as plt
from math import pow
from typing import Dict, Optional, Tuple
from .access import ( load, timelines, TimeLine, Province_State,
    Country_Region, mktimeline, walk, timedelta )
from .fetch import ( REGIONS_EN_RU, COVID19RU_PENDING, fetch_pending,
    pending_timestamp )
from itertools import chain
from datetime import datetime
from collections import OrderedDict

import locale
locale.setlocale(locale.LC_TIME, "en_US")



def plot_pending_changes():
  data=[]
  trh=datetime(2020,5,1)
  for root, dirs, filenames in walk(COVID19RU_PENDING, topdown=True):
    for filename in sorted(filenames):
      if filename.endswith('json') and pending_timestamp(filename)>trh:
        pd=fetch_pending(filename)
        data.append((pd.utcnow, int(pd.val['Москва']['cases'])))
  dates1,numbers=zip(*sorted(data))
  plt.plot(dates1,numbers,marker='o',label='Avaliable measurements')

  data=[]
  dfs=load(country_region='Russia')
  # print(list(dfs.values()[0])
  for date,df in dfs.items():
    if date>trh:
      row=df[df['Province_State']=='Moscow'].iloc[0]
      dt=datetime.fromisoformat(row['Last_Update'])
      cnf=int(row['Confirmed'])
      data.append((dt,cnf))
  dates2,numbers=zip(*sorted(data))
  plt.plot(dates2,numbers,marker='o',label='Included by us (as previous day)')

  data=[]
  for date,df in dfs.items():
    if date>trh:
      row=df[df['Combined_Key']=='Russia'].iloc[0]
      dt=datetime.fromisoformat(row['Last_Update'])
      cnf=int(row['Confirmed'])
      data.append((dt,cnf))
  dates3,numbers=zip(*sorted(data))
  # plt.plot(dates3,numbers,marker='o')
  args={'label':'Included by the Upstream'}
  for date in dates3:
    plt.axvline(date,0,1,color='green',**args)
    args={}

  date=min(dates1+dates2).replace(hour=0, minute=0, second=0, microsecond=0)
  args={'label':'UTC midnight'}
  while date<max(dates1+dates2):
    plt.axvline(date,0,1,color='grey', alpha=0.5, **args)
    date+=timedelta(days=1)
    args={}
  plt.legend(loc='upper left')
  plt.title("Monitoring details (data is for Moscow, Russia)")



def timelines_merge(tls, key1, key2, key_out):
  def _todict(tl:TimeLine)->dict:
    return {date:(c,d,r) for date,c,d,r in zip(tl.dates,tl.confirmed,tl.deaths,tl.recovered)}

  tl_m=_todict(tls[key1])
  tl_mo=_todict(tls[key2])

  dates=[]; cs=[]; ds=[]; rs=[]
  for d in sorted(list(set(tl_m.keys()).union(set(tl_mo.keys())))):
    dates.append(d)
    rm=tl_m.get(d,(0,0,0))
    rmo=tl_mo.get(d,(0,0,0))
    cs.append(rm[0]+rmo[0])
    ds.append(rm[1]+rmo[1])
    rs.append(rm[2]+rmo[2])
  tls[key_out]=mktimeline(dates,cs,ds,rs)
  del tls[key1]
  del tls[key2]
  return tls

def timelines_preprocess(tls)->Dict[Tuple[Province_State,Country_Region],TimeLine]:
  """ Merge Moscow and Moscow oblast """
  timelines_merge(tls, ('Moscow','Russia'), ('Moscow oblast','Russia'), ('Moscow+MO','Russia'))
  timelines_merge(tls, ('Saint Petersburg','Russia'), ('Leningradskaya oblast','Russia'), ('SPb+LO','Russia'))
  del tls[('','Russia')]
  return tls

def plot(labels_in_russian:bool=True, **kwargs):
  if labels_in_russian:
    plot_(
      metric_fn=lambda tl:tl.confirmed,
      title="Число подтвержденных случаев COVID19 в регионах России на {lastdate}{title_suffix}",
      xlabel="Количество дней с момента {min_threshold}-го подтвержденного случая",
      ylabel="Подтвержденных случаев",
      labels_in_russian=labels_in_russian,
      plot_scale_markers=False,
      **kwargs)
  else:
    plot_(
      metric_fn=lambda tl:tl.confirmed,
      title="Confirmed COVID19 cases in regions of Russia, as of {lastdate}{title_suffix}",
      xlabel="Number of days since {min_threshold}th confirmed",
      ylabel="Confirmed cases",
      labels_in_russian=labels_in_russian,
      plot_scale_markers=False,
      **kwargs)

def plot_sliding(labels_in_russian:bool=True, **kwargs):
  if labels_in_russian:
    plot_(
      metric_fn=lambda tl:tl.daily_cases_ma7,
      title="Скользящее среднее суточного числа заражений COVID19 в регионах России на {lastdate} за семь дней{title_suffix}",
      xlabel="Количество дней с момента превышения значения {min_threshold} заражений в сутки",
      ylabel="Суточное число заражений, среднее за 7 дней",
      labels_in_russian=labels_in_russian,
      plot_scale_markers=False,
      **kwargs)
  else:
    plot_(
      metric_fn=lambda tl:tl.daily_cases_ma7,
      title="Moving average daily confirmed COVID19 cases in regions of Russia, as of {lastdate}, averaged for 7 days{title_suffix}",
      xlabel="Number of days since above {min_threshold}",
      ylabel="Daily confirmed case, 7-days moving average",
      labels_in_russian=labels_in_russian,
      plot_scale_markers=False,
      **kwargs)

def plot_(metric_fn,
         xlabel:str,
         ylabel:str,
         title:str,
         min_threshold=100,
         show:bool=False,
         save_name:Optional[str]=None,
         labels_in_russian:bool=False,
         right_margin:int=5,
         rng:Tuple[Optional[int],Optional[int]]=(None,None),
         title_suffix:str='',
         plot_scale_markers:bool=True
         )->None:
  plt.figure(figsize=(16, 6))
  plt.yscale('log')

  max_tick=0
  min_metric=99999999
  tls=timelines_preprocess(timelines(country_region='Russia', default_loc=''))
  # tls=timelines(country_region='US', default_loc='')
  tls_list=sorted(tls.items(), key=lambda i:-metric_fn(i[1])[-1])
  tls_list=tls_list[rng[0]:rng[1]]
  out:Dict[Tuple[str,str],TimeLine]=OrderedDict()
  out.update({k:v for k,v in tls_list})
  out[('', 'Italy (ref)')]=list(timelines(country_region='Italy', default_loc='').values())[0]
  out[('', 'Japan (ref)')]=list(timelines(country_region='Japan', default_loc='').values())[0]
  out[('', 'Ukraine (ref)')]=list(timelines(country_region='Ukraine', default_loc='').values())[0]
  out[('', 'Belarus (ref)')]=list(timelines(country_region='Belarus', default_loc='').values())[0]
  if ('Moscow+MO','Russia') not in out and ('Moscow+MO','Russia') in tls:
    out.update({('Moscow+MO (ref)','Russia'):tls[('Moscow+MO','Russia')]})
  lastdate=out[tls_list[0][0]].dates[-1]

  # print(out.keys())

  # Calculate total number of days to show
  leaders_days_after_threshold=0
  threshold=False
  for (ps,cr),tl in tls_list:
    ticks=0; threshold=False
    for c in metric_fn(tl):
      if c>min_threshold:
        threshold=True
      if threshold:
        ticks+=1
    if ticks>leaders_days_after_threshold:
      leaders_days_after_threshold=ticks
  leaders_days_after_threshold+=right_margin

  for (ps,cr),tl in out.items():
    # Skip whole Russia which is similar to Moscow
    if len(ps)==0 and cr=='Russia':
      continue
    # Skip low-data regions
    # if metric_fn(tl)[-1]<10:
    #   continue

    threshold=False
    ticks=[]; tick=0; metric=[]
    for d,c in zip(tl.dates,metric_fn(tl)):
      if c>min_threshold:
        threshold=True
      if not threshold:
        continue
      if tick>leaders_days_after_threshold:
        break
      ticks.append(tick)
      metric.append(c)
      tick+=1

    if len(metric)==0:
      continue
    max_tick=max(max_tick,tick)
    min_metric=min(min_metric,metric[0])

    if labels_in_russian:
      label={'Moscow+MO':'Москва+область',
             'Moscow+MO (ref)':'Москва+область (справ.)',
             'SPb+LO':'CПб+область'}.get(ps,REGIONS_EN_RU.get(ps))
      if label is None:
        label={'Russia':'Россия',
               'Italy (ref)':'Италия (справ.)',
               'Japan (ref)':'Япония (справ.)',
               'Ukraine (ref)':'Украина (справ.)',
               'Belarus (ref)':'Белоруссия (справ.)',
               }.get(cr,cr)
    else:
      label=ps or cr
    label+=f" ({int(metric_fn(tl)[-1])})"

    alpha=0.6 if cr in ['Italy (ref)','Japan (ref)'] else 1.0
    color={'Italy (ref)':'#d62728',
           'Japan (ref)':'#9467bd',
           'Ukraine (ref)':'#9407bd',
           'Belarus (ref)':'#94670d',
           }.get(cr)
    ls={'Moscow+MO (ref)':':'}.get(ps,
          {'Italy (ref)':':',
           'Japan (ref)':':',
           'Ukraine (ref)':':',
           'Belarus (ref)':':',
          }.get(cr))
    p=plt.plot(ticks, metric, label=label, alpha=alpha, color=color, linestyle=ls)

  def _growth_rate_label(x):
    if labels_in_russian:
      return f'Прирост {x}%'
    else:
      return f'{x}% growth rate'

  if plot_scale_markers:
    plt.plot(range(max_tick),[min_metric*pow(1.05,x) for x in range(max_tick)],
             color='grey', linestyle='--', label=_growth_rate_label(5), alpha=0.5)
    plt.plot(range(max_tick),[min_metric*pow(1.3,x) for x in range(max_tick)],
             color='grey', linestyle='--', label=_growth_rate_label(30), alpha=0.5)
  # plt.plot(range(max_tick),[min_metric*pow(1.85,x) for x in range(max_tick)],
  #          color='grey', linestyle='--', label=_growth_rate_label(85), alpha=0.5)

  plt.title(title.format(lastdate=lastdate.strftime('%d.%m.%Y'), title_suffix=title_suffix))
  plt.xlabel(xlabel.format(min_threshold=min_threshold))
  plt.ylabel(ylabel)

  from matplotlib.font_manager import FontProperties
  fontP = FontProperties()
  fontP.set_size('x-small')

  plt.grid(True)
  plt.legend(loc='upper left', prop=fontP, ncol=2)

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
