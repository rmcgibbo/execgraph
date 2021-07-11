{ lib
, buildPythonPackage
, pytestCheckHook
, rustPlatform
, maturin
, numpy
, scipy
, networkx
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
    sha256 = "sha256-2sYu7leg9ZpplucdYapBFfSYvOkwvb8+1j0RP+cmK3k=";
  };

  nativeBuildInputs = with rustPlatform; [
    cargoSetupHook
    maturinBuildHook
  ];

  postInstall = ''
    cargo test --verbose --no-default-features
  '';

  checkInputs = [
    pytestCheckHook
    numpy
    scipy
    networkx
  ] ;
  pythonImportsCheck = [ "_execgraph" ];
}