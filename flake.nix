{
  description = "Parallel execution of shell commands with DAG dependencies";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    py-utils.url = "github:rmcgibbo/python-flake-utils";
    utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.inputs.flake-utils.follows = "utils";
  };
  outputs = { self, nixpkgs, utils, py-utils, naersk, rust-overlay }: {
    overlay = py-utils.lib.mkPythonOverlay (pkgs: {
      execgraph = pkgs.callPackage ./. { inherit naersk; };
    });
  } //
  utils.lib.eachSystem ["x86_64-linux"] (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ self.overlay (import rust-overlay) ];
      };
    in
    {
      packages.default = pkgs.python310.pkgs.execgraph;

      devShells.default = pkgs.mkShell rec {
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
