#!/usr/bin/env python

from covid19ru.plot import plot

plot(show=False, save_name='ruscovid.png', labels_in_russian=False)
plot(show=True, save_name='ruscovid_ru.png', labels_in_russian=True)

