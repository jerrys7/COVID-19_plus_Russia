#!/usr/bin/env python

from covid19ru.plot import plot

def plot_all():
  plot(show=False, save_name='ruscovid.png', labels_in_russian=False, confirmed_min_threshold=30, rng=(None,10))
  plot(show=False, save_name='ruscovid_10_20.png', labels_in_russian=False, confirmed_min_threshold=30, rng=(10,20))
  plot(show=True, save_name='ruscovid_ru.png', labels_in_russian=True, confirmed_min_threshold=30, rng=(None,10))
  plot(show=True, save_name='ruscovid_ru_10_20.png', labels_in_russian=True, confirmed_min_threshold=30, rng=(10,20))

if __name__ == '__main__':
  plot_all()

