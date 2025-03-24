{
  description = "Dev environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      utils,
      nixpkgs,
      ...
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
      in
      {
        devShells.default =
          with pkgs;
          mkShell {
            packages =
              let
                python = python310.withPackages (
                  p: with p; [
                    pip
                    virtualenv
                  ]
                );
              in
              [
                python
              ];

            LD_LIBRARY_PATH = lib.makeLibraryPath [
              stdenv.cc.cc.lib
              zlib
            ];
          };
      }
    );
}
