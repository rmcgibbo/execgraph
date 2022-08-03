{
  description = "Parallel execution of shell commands with DAG dependencies";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.05";
  inputs.py-utils.url = "github:rmcgibbo/python-flake-utils";
  inputs.utils.url = "github:numtide/flake-utils";
  inputs.crane.url = "github:ipetkov/crane?rev=8261aaeb8f47958a4f8c11c375a66e0d9a7e8927";
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
      defaultPackage = pkgs.python3.pkgs.execgraph;

      devShell = pkgs.mkShell rec {
        buildInputs = with pkgs; with python39Packages; [
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
