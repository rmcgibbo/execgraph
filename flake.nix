{
  description = "Parallel execution of shell commands with DAG dependencies";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.py-utils.url = "github:rmcgibbo/python-flake-utils";
  inputs.utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, utils, py-utils }: {
    overlay = py-utils.lib.mkPythonOverlay (pkgs: {
      execgraph = pkgs.callPackage ./. { };
    });
  } //
  utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ self.overlay ];
      };
    in
    {
      defaultPackage = pkgs.python3.pkgs.execgraph;

      devShell = pkgs.mkShell rec {
        name = "execgraph";
        shellHook = ''
          export PS1="\n(${name}) \[\033[1;32m\][\[\e]0;\u@\h: \w\a\]\u@\h:\w]\[\033[0m\]\n$ "
        '';
        buildInputs = with pkgs; with python39Packages; [
          cargo
          rustc
          clippy
          maturin
          cargo-udeps
          cargo-tarpaulin

          rustfmt
          black
          isort
          mypy

          # for py.test
          pytest
          networkx
          numpy
          scipy
          curl

          # for tokio-console
          protobuf
        ];
      };
    });
}
