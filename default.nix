{ lib
, system
, buildPythonPackage
, pytest
, python
, numpy
, scipy
, networkx
, curl
, rust-bin
, naersk
, toxiproxy
, procps
, pstree
, stdenv
, darwin
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
    "build.rs"
  ];

  naerskLib = naersk.lib."${system}".override {
    rustc = rust-bin.stable.latest.default;
    cargo = rust-bin.stable.latest.default;
  };
  execgraph = naerskLib.buildPackage {
    inherit src;
    name = "execgraph";
    version = "0.1.0";

    nativeBuildInputs = [
      python
    ] ++ lib.optionals (stdenv.isDarwin) [
      darwin.apple_sdk.frameworks.CoreServices
      darwin.apple_sdk.frameworks.SystemConfiguration
    ];
    copyLibs = true;
    singleStep = true;
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
    ln -s ${execgraph}/lib/libexecgraph${stdenv.hostPlatform.extensions.sharedLibrary} $out/${python.sitePackages}/execgraph.so

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
    procps
    pstree
  ];
  pythonImportsCheck = [ "execgraph" ];
}
