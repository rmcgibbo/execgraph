{ lib
, buildPythonPackage
, pytestCheckHook
, rustPlatform
, maturin
, numpy
, scipy
, networkx
, curl
}:

let
  filterSrcByPrefix = src: prefixList:
    lib.cleanSourceWith {
      filter = (path: type:
        let relPath = lib.removePrefix (toString ./. + "/") (toString path);
        in lib.any (prefix: lib.hasPrefix prefix relPath) prefixList);
      inherit src;
    };

in buildPythonPackage rec {
  pname = "execgraph";
  version = "0.1.0";
  format = "pyproject";

  src = filterSrcByPrefix ./. [
    "pyproject.toml"
    "src"
    "Cargo.lock"
    "Cargo.toml"
    "tests"
  ];

  cargoDeps = rustPlatform.fetchCargoTarball {
    inherit src;
    name = "${pname}-${version}";
    sha256 = "sha256-CYWLPJENSMAoMfM9UTWUFQNWlxAbJHZVwhV8Nma3KKA=";
    # sha256 = "0000000000000000000000000000000000000000000000000000";
  };

  nativeBuildInputs = with rustPlatform; [
    cargoSetupHook
    rust.rustc
    rust.cargo
    maturinBuildHook
  ];

  RUST_BACKTRACE = "full";
  CARGO_INCREMENTAL = "0";
  CARGO_PROFILE_RELEASE_LTO = "thin";

  preBuild = ''
    cargo build -j $NIX_BUILD_CORES \
      --frozen \
      --release \
      --bins

    mkdir -p $out/bin
    install -Dv target/release/execgraph-remote $out/bin/
  '';

  preCheck = ''
    export PATH=$out/bin:$PATH
  '';

  checkInputs = [
    pytestCheckHook
    numpy
    scipy
    networkx
    curl
  ];
  pytestFlagsArray = [ "-s" ];
  pythonImportsCheck = [ "execgraph" ];
}