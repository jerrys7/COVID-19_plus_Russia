#{ pkgs ?  import ./lib/nixpkgs {}
{ pkgs ?  import <nixpkgs> {}
, stdenv ? pkgs.stdenv
} :
let

  self = pkgs.python37Packages;
  pyls = self.python-language-server.override { providers=["pycodestyle" "pyflakes"]; };
  pyls-mypy = self.pyls-mypy.override { python-language-server=pyls; };

  be = stdenv.mkDerivation {
    name = "buildenv";
    buildInputs =
    (
      with self;
    [
      ipython
      numpy
      pandas
      matplotlib

      pyls-mypy
      pyls
      jupyter
    ]);

    shellHook = with pkgs; ''
      export QT_QPA_PLATFORM_PLUGIN_PATH=`echo ${pkgs.qt5.qtbase.bin}/lib/qt-*/plugins/platforms/`
      if test -f ./env.sh ; then
        . ./env.sh
      fi
    '';
  };

in
  be
