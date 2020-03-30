import matplotlib.pyplot as plt
from math import pow
from .access import load, timelines
from itertools import chain
from datetime import datetime

def plot(confirmed_min_threshold=30, show:bool=False)->None:
  plt.figure(figsize=(16, 6))
  plt.yscale('log')

  max_tick=0
  min_confirmed=99999999
  out={}
  out.update(timelines(country_region='Russia'))
  out.update(timelines(country_region='Italy'))
  out.update(timelines(country_region='Japan'))
  ndays_in_russia_after_threshold=(out['Moscow','Russia'].dates[-1]-out['Moscow','Russia'].dates[0]).days-5

  for (ps,cr),tl in out.items():
    print(ps,cr)
    if ps is None and cr=='Russia':
      continue # Skip whole Russia which is similar to Moscow
    if tl.confirmed[-1]<10:
      continue

    ticks=[]; tick=0; confirmed=[];
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

    plt.plot(ticks, confirmed, label=f'{ps or cr}')


  plt.plot(range(max_tick),[min_confirmed*pow(1.05,x) for x in range(max_tick)],
           color='grey', linestyle='--', label='5% groth rate', alpha=0.5)
  plt.plot(range(max_tick),[min_confirmed*pow(1.3,x) for x in range(max_tick)],
           color='grey', linestyle='--', label='30% groth rate', alpha=0.5)
  plt.plot(range(max_tick),[min_confirmed*pow(1.85,x) for x in range(max_tick)],
           color='grey', linestyle='--', label='85% groth rate', alpha=0.5)
  plt.xlabel(f"Number of days since {confirmed_min_threshold}th confirmed")
  plt.title("Confirmed cases")
  plt.grid(True)
  plt.legend(loc='upper left')
  plt.savefig('ruscovid.png')
  print('Saved to ruscovid.png')
  if show:
    plt.show()

if __name__ == '__main__':
  plot()
