{
  description = "A Nix-flake-based Go 1.24 development environment";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1.*.tar.gz";

    nur = {
      url = "github:nix-community/NUR";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, nur }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forEachSupportedSystem = f:
        nixpkgs.lib.genAttrs supportedSystems (system:
          let
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ nur.overlays.default ];
              config.allowUnfreePredicate = pkg: builtins.elem (nixpkgs.lib.getName pkg) [
                "goreleaser-pro"
              ];
            };
          in
          f { pkgs = pkgs; system = system; }
        );

      speakeasyVersion = "1.563.0";
      speakeasyPlatforms = {
        "x86_64-linux"   = "linux_amd64";
        "aarch64-linux"  = "linux_arm64";
        "x86_64-darwin"  = "darwin_amd64";
        "aarch64-darwin" = "darwin_arm64";
      };
      speakeasyHashes = {
        "x86_64-linux"   = "632559a6bdc765ef42b81b8404fd0a3e5023919a0bb70ff7e40a8cc259545afd";
        "aarch64-linux"  = "c74c502df3a05a2d69e6b282886df23354a319d0510d2c1a21fcc124b7ad00ef";
        "x86_64-darwin"  = "8814be1fdd4eaf6dcc7fb251ede5693e1d3d4c8e03986f8d37bfd59e049698b9";
        "aarch64-darwin" = "12c20fa485de4725c9730cb2e8936cab4b0961d0a956e9f4c45534371f2a6148";
      };

    in
    {
      packages = forEachSupportedSystem ({ pkgs, system }:
        {
          speakeasy = pkgs.stdenv.mkDerivation {
            pname = "speakeasy";
            version = speakeasyVersion;

            src = pkgs.fetchurl {
              url = "https://github.com/speakeasy-api/speakeasy/releases/download/v${speakeasyVersion}/speakeasy_${speakeasyPlatforms.${system}}.zip";
              sha256 = speakeasyHashes.${system};
            };

            nativeBuildInputs = [ pkgs.unzip ];
            dontUnpack = true;

            installPhase = ''
              mkdir -p $out/bin
              unzip $src
              ls -al
              install -m755 speakeasy $out/bin/
            '';

            name = "speakeasy";
          };
        }
      );

      defaultPackage.x86_64-linux   = self.packages.x86_64-linux.speakeasy;
      defaultPackage.aarch64-linux  = self.packages.aarch64-linux.speakeasy;
      defaultPackage.x86_64-darwin  = self.packages.x86_64-darwin.speakeasy;
      defaultPackage.aarch64-darwin = self.packages.aarch64-darwin.speakeasy;

      devShells = forEachSupportedSystem ({ pkgs, system }:
        let
          stablePackages = with pkgs; [
            ginkgo
            go_1_24
            go-tools
            golangci-lint
            gomarkdoc
            goperf
            gotools
            jdk11
            jq
            just
            mockgen
            nodejs_22
            protobuf_27
            protoc-gen-go
            protoc-gen-go-grpc
            yq-go
          ];
          otherPackages = [
            pkgs.nur.repos.goreleaser.goreleaser-pro
            self.packages.${system}.speakeasy
          ];
        in
        {
          default = pkgs.mkShell {
            packages = stablePackages ++ otherPackages;
          };
        }
      );
    };
}
