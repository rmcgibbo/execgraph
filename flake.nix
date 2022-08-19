{
  description = "Parallel execution of shell commands with DAG dependencies";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.05";
  inputs.py-utils.url = "github:rmcgibbo/python-flake-utils";
  inputs.utils.url = "github:numtide/flake-utils";
  inputs.crane.url = "github:ipetkov/crane";
  inputs.crane.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { self, nixpkgs, utils, py-utils, crane }: {
    overlay = py-utils.lib.mkPythonOverlay (pkgs: {
      execgraph = pkgs.callPackage ./. { inherit crane; };
    });
  } //
  utils.lib.eachSystem ["x86_64-linux"] (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ self.overlay ];
      };
    in
    {
      defaultPackage = pkgs.python310.pkgs.execgraph;

      devShell = pkgs.mkShell rec {
        buildInputs = with pkgs; with python310Packages; [
          cargo
          rustc
          clippy
          maturin
          cargo-flamegraph
          cargo-udeps
          cargo-edit
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
          toxiproxy
        ] ++ [ pkgs.coz ];
      };
    });
}
