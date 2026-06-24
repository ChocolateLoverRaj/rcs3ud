{
  description = "A devShell example";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        craneLib = crane.mkLib pkgs;
      in
      {
        devShells.default =
          with pkgs;
          mkShell {
            buildInputs = [
              rust-bin.stable.latest.default
              rust-analyzer
              awscli2
              # For the crc32 command
              toybox
            ];
          };
        packages.default = craneLib.buildPackage {
          pname = "rcs3ud_cli";
          src = craneLib.cleanCargoSource ./.;
          cargoExtraArgs = "-p rcs3ud_cli";
          buildInputs = with pkgs; [
            rustPlatform.bindgenHook
          ];
        };
      }
    );
}
