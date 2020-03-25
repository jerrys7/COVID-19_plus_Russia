from os import (
    mkdir, makedirs, replace, listdir, rmdir, symlink, rename, remove, environ,
    walk, lstat, chmod, stat, readlink )
from os.path import (
    basename, join, isfile, isdir, islink, relpath, abspath, dirname, split,
    getsize )

from pandas import DataFrame, read_csv

from typing import ( Any, List, Dict, Tuple, NamedTuple )

COVID19RU_ROOT=join('..','csse_covid_19_data','csse_covid_19_daily_reports')

Error=NamedTuple('Error',[('file',str),('text',str)])

def check_file(filepath:str)->List[Error]:
  try:
    print(f'Checking {basename(filepath)}', end='')
    df=read_csv(filepath)
  except KeyboardInterrupt:
    raise
  except Exception as e:
    print('.....ERROR')
    return [Error(filepath,str(e))]
  print('.....OK')
  return []

def check_all(root:str=COVID19RU_ROOT)->List[Error]:
  errors=[]
  for root, dirs, filenames in walk(abspath(root), topdown=True):
    for filename in sorted(filenames):
      if filename.endswith('csv'):
        filepath=abspath(join(root, filename))
        errors.extend(check_file(filepath))
  return errors


if __name__ == '__main__':
  errors=check_all()
  assert len(errors)==0
