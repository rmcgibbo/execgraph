{
  description = "Parallel execution of shell commands with DAG dependencies";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-21.05";
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
      devShell = pkgs.mkShell {
        buildInputs = with pkgs; with python39Packages; [
          cargo
          rustc
          maturin

          rustfmt
          black
          isort
          mypy
        ];
      };
    });
}
