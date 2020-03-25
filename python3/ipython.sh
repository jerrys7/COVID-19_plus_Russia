#!/bin/sh

if ! test -d "$CWD" ; then
  echo "CWD is not set"
  exit 1
fi

# if test -n "$DISPLAY"; then
#   export QT_QPA_PLATFORM_PLUGIN_PATH=`echo ${pkgs.qt5.qtbase.bin}/lib/qt-*/plugins/platforms/`
#   alias ipython='ipython --matplotlib=qt5 --profile-dir=$CWD/.ipython-profile'
#   alias ipython0='ipython --profile-dir=$CWD/.ipython-profile'
# fi

mkdir $CWD/.ipython-profile 2>/dev/null || true
cat >$CWD/.ipython-profile/ipython_config.py <<EOF
c = get_config()
c.InteractiveShellApp.exec_lines = []
c.InteractiveShellApp.exec_lines.append('%load_ext autoreload')
c.InteractiveShellApp.exec_lines.append('%autoreload 2')

def tweak():
  print("Enabling tweaks")

  try:
    import matplotlib;
    matplotlib.use('Qt5Agg');
    import matplotlib.pyplot;
    matplotlib.pyplot.ioff()
  except Exception as e:
    print("Failed to tweak matplotlib. Is it installed?")

tweak()
EOF

ipython3 --profile-dir=$CWD/.ipython-profile -i "$@"
