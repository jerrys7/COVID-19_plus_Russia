export CWD=`pwd`
# export TERM=xterm-256color # TODO: check and document
# export PYTHONPATH=""
export MYPYPATH=""
for p in \
  $CWD/src \
  ; do
  if test -d "$p" ; then
    export PYTHONPATH="$p:$PYTHONPATH"
    export MYPYPATH="$p:$MYPYPATH"
    echo "Directory '$p' added to PYTHONPATH" >&2
  else
    echo "Directory '$p' doesn't exists. Not adding to PYTHONPATH" >&2
  fi
done

alias ipython="sh $CWD/ipython.sh"
