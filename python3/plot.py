#!/usr/bin/env python

from covid19ru.plot import plot

def plot_all():
  plot(show=False, save_name='ruscovid.png', labels_in_russian=False,
       confirmed_min_threshold=30, rng=(None,10), title_suffix=', Top-10')
  plot(show=False, save_name='ruscovid_10_20.png', labels_in_russian=False,
       confirmed_min_threshold=30, rng=(10,20), title_suffix=', Places 11..21')
  plot(show=True, save_name='ruscovid_ru.png', labels_in_russian=True,
       confirmed_min_threshold=30, rng=(None,10), title_suffix=', первая десятка')
  plot(show=True, save_name='ruscovid_ru_10_20.png', labels_in_russian=True,
       confirmed_min_threshold=30, rng=(10,20), title_suffix=', места 11..21')

if __name__ == '__main__':
  plot_all()

