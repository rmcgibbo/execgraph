{ lib
, system
, buildPythonPackage
, pytest
, python
, numpy
, scipy
, networkx
, curl
, crane
, toxiproxy
}:

let
  filterSrcByPrefix = src: prefixList:
    lib.cleanSourceWith {
      filter = (path: type:
        let relPath = lib.removePrefix (toString ./. + "/") (toString path);
        in lib.any (prefix: lib.hasPrefix prefix relPath) prefixList);
      inherit src;
    };
  src = filterSrcByPrefix ./. [
    "pyproject.toml"
    "src"
    "Cargo.lock"
    "Cargo.toml"
    "tests"
  ];

  # Build *just* the cargo dependencies, since they don't change super often
  craneLib = crane.lib.${system};
  cargoArtifacts = craneLib.buildDepsOnly {
    inherit src;
    name = "execgraph-deps";
    version = "0.1.0";
    nativeBuildInputs = [
      python
    ];
    doCheck = false;
  };
  execgraph = craneLib.buildPackage {
    inherit cargoArtifacts src;
    name = "execgraph";
    version = "0.1.0";
    nativeBuildInputs = [
      python
    ];
    doCheck = false;
  };

in buildPythonPackage rec {
  pname = "execgraph";
  version = "0.1.0";
  format = "other";
  inherit src;

  nativeBuildInputs = [
    execgraph
  ];

  installPhase = ''
    mkdir -p $out/bin
    mkdir -p $out/${python.sitePackages}
    ln -s ${execgraph}/bin/execgraph-remote $out/bin/
    ln -s ${execgraph}/lib/libexecgraph.so $out/${python.sitePackages}/execgraph.so

    export PYTHONPATH="$out/${python.sitePackages}:$PYTHONPATH"
    export PATH=$out/bin:$PATH
  '';

  checkPhase = ''
    py.test -vvv
  '';

  checkInputs = [
    toxiproxy
    pytest
    numpy
    scipy
    networkx
    curl
  ];
  pythonImportsCheck = [ "execgraph" ];
}
