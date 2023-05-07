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
    overlays.default = py-utils.lib.mkPythonOverlay (pkgs: {
      execgraph = pkgs.callPackage ./. { inherit naersk; };
    });
  } //
  utils.lib.eachSystem ["x86_64-linux" "aarch64-darwin"] (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ self.overlays.default (import rust-overlay) ];
      };
    in
    {
      packages.default = pkgs.python310.pkgs.execgraph;

      devShells.default = pkgs.mkShell rec {
        name = "execgraph";
        shellHook = ''
          export PS1="\n(${name}) \[\033[1;32m\][\[\e]0;\u@\h: \w\a\]\u@\h:\w]\[\033[0m\]\n$ "
        '';
        buildInputs = with pkgs; with python310Packages; [
          # https://github.com/oxalica/rust-overlay#use-in-devshell-for-nix-develop
          (pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" "rust-std" ];
          })
          cargo-flamegraph
          cargo-udeps
          cargo-edit

          rustfmt
          isort
          mypy

          # for py.test
          pytest
          networkx
          numpy
          scipy
          curl
          toxiproxy
        ] ++ pkgs.lib.optionals (pkgs.stdenv.isLinux) [
          black # broken on macos                    
        ] ++ pkgs.lib.optionals (pkgs.stdenv.isDarwin) [
            darwin.apple_sdk.frameworks.SystemConfiguration
            darwin.apple_sdk.frameworks.CoreServices
	          libiconv
        ];
      };
    });
}
