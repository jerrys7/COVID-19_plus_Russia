from os import (
    mkdir, makedirs, replace, listdir, rmdir, symlink, rename, remove, environ,
    walk, lstat, chmod, stat, readlink )
from os.path import (
    basename, join, isfile, isdir, islink, relpath, abspath, dirname, split,
    getsize, splitext )

from .defs import COVID19RU_ROOT
from pandas import DataFrame, read_csv, notnull
from datetime import datetime
from typing import ( Any, List, Dict, Tuple, NamedTuple, Optional )
from collections import defaultdict

Error=NamedTuple('Error',[('file',str),('text',str)])

class CheckerState:
  def __init__(self)->None:
    self.prev:Dict[int,Optional[DataFrame]]=defaultdict(lambda:None) # Last seen data of certain format


def filedate(filepath:str)->datetime:
  return datetime.strptime(splitext(basename(filepath))[0],"%m-%d-%Y")

def is_format1(f:str):
  return filedate(f)<datetime(2020,3,22)

def is_format2(f:str):
  return filedate(f)>=datetime(2020,3,22)

def filter_ru(df:DataFrame)->DataFrame:
  return df[(df['Country_Region']=='Russia') & notnull(df['Province_State'])]

def check_file(filepath:str, cs:CheckerState)->List[Error]:
  fmt=1
  try:
    fn=basename(filepath)
    print(f'Checking {fn}', end='')
    df=read_csv(filepath)
    if is_format1(filepath):
      fmt=1
      print('.....skipping',end='')
    elif is_format2(filepath):
      fmt=2
      ru=filter_ru(read_csv(filepath))
      prev=cs.prev[fmt]
      if prev is not None:
        prev_ru=filter_ru(prev)
        prev_regions=len(prev_ru.index)
        num_regions=len(ru.index)
        assert num_regions>=prev_regions, f'Number of regioins decreased!  {num_regions} < {prev_regions}'
        assert len(ru[ru['Confirmed']>=0].index)==num_regions, 'ill-formed confirmed'
        assert len(ru[ru['Deaths']>=0].index)==num_regions, 'ill-formed deaths'
        assert len(ru[ru['Recovered']>=0].index)==num_regions, 'ill-formed recovered'

        new_regions=False
        for i,row in ru.iterrows():
          region=row['Province_State']
          p=prev[prev['Province_State']==region]
          if len(p.index)>0:
            prow=p.iloc[0]
            assert row['Confirmed'] >= prow['Confirmed'], \
                f'Confirmed decreased for {region}'
            assert row['Deaths'] >= prow['Deaths'], \
                f'Resurrected in {region}??'
            assert row['Recovered'] >= prow['Recovered'], f'Recovered decreased in {region} (oh no!)'
          else:
            new_regions=True
        if new_regions:
          print('.....newregions', end='')
      else:
        print('.....noprev', end='')
    else:
      raise ValueError('Unknown format')
    cs.prev[fmt]=df
    print('.....OK')
    return []
  except KeyboardInterrupt:
    cs.prev[fmt]=None
    raise
  except Exception as e:
    cs.prev[fmt]=None
    print('.....ERROR')
    return [Error(filepath,str(e))]

def check_all(root:str=COVID19RU_ROOT)->List[Error]:
  cs=CheckerState()
  errors=[]
  for root, dirs, filenames in walk(abspath(root), topdown=True):
    for filename in sorted(filenames):
      if filename.endswith('csv'):
        filepath=abspath(join(root, filename))
        errors.extend(check_file(filepath,cs))
  return errors


if __name__ == '__main__':
  errors=check_all()
  assert len(errors)==0
