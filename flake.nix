{
  description = "A Nix-flake-based Go 1.27 development environment";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";
    nixpkgs-unstable.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";

    nur = {
      url = "github:nix-community/NUR";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, nur }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      # Match the generator that produced platform-ui/packages/sdks/ledger.
      speakeasyVersion = "1.762.0";
      speakeasyPlatforms = {
        "x86_64-linux" = "linux_amd64";
        "aarch64-linux" = "linux_arm64";
        "x86_64-darwin" = "darwin_amd64";
        "aarch64-darwin" = "darwin_arm64";
      };
      speakeasyHashes = {
        "x86_64-linux" = "c3c8961186df89f14a7f9971e856c6188394b2b70ececdccf61b39ff46d47e30";
        "aarch64-linux" = "569fc0e2628aee870436292ec2c5ab6bb2854fd3536997f17099b899a61af5a9";
        "x86_64-darwin" = "38b44af7e03d9a3899e722840ee75795116d7d95f99313e85a60dc4c6cc0cd49";
        "aarch64-darwin" = "5f02fc234f3063d4ffa430ae227859fe06f2284096e3dcbffaab4f1fa2b7aa32";
      };

      forEachSupportedSystem = f:
        nixpkgs.lib.genAttrs supportedSystems (system:
          let
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ nur.overlays.default ];
              config.allowUnfreePredicate = pkg: builtins.elem (nixpkgs.lib.getName pkg) [
                "acli"
                "acli-unwrapped"
                "goreleaser-pro"
              ];
            };
            pkgs-unstable = import nixpkgs-unstable {
              inherit system;
            };
          in
          f { pkgs = pkgs; pkgs-unstable = pkgs-unstable; system = system; }
        );

    in
    {
      packages = forEachSupportedSystem ({ pkgs, pkgs-unstable, system }: {
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
            install -m755 speakeasy $out/bin/
          '';
        };
      });

      devShells = forEachSupportedSystem ({ pkgs, pkgs-unstable, system }:
        let
          # Mock generation must understand the root module Go version.
          buildGoModule = pkgs.buildGoModule.override { go = pkgs.go_1_27; };
          stablePackages = with pkgs; [
            acli
            go_1_27
            ffmpeg
            ginkgo
            gomarkdoc
            go-jsonnet
            jsonnet-bundler
            grpcurl
            jdk11
            jq
            k6
            kubernetes-helm
            nodejs_22
            oras
            python314
            trufflehog
            uv
            vhs
            yq-go
          ];
          unstablePackages = with pkgs-unstable; [
            go-tools
            protobuf_34
            goperf
            gotools
            (mockgen.override { inherit buildGoModule; })
            protoc-gen-go
            protoc-gen-go-grpc
            protoc-gen-go-vtproto
            golangci-lint
            setup-envtest
            just
          ];
          otherPackages = [
            self.packages.${system}.speakeasy
            pkgs.nur.repos.goreleaser.goreleaser-pro
          ];
        in
        {
          default = pkgs.mkShell {
            packages = stablePackages ++ unstablePackages ++ otherPackages;

            shellHook = ''
              # Auto-configure envtest assets for operator integration tests.
              # setup-envtest downloads etcd + kube-apiserver on first run and caches them.
              if [ -z "$KUBEBUILDER_ASSETS" ]; then
                KUBEBUILDER_ASSETS="$(setup-envtest use -p path 2>/dev/null || true)"
                if [ -n "$KUBEBUILDER_ASSETS" ]; then
                  export KUBEBUILDER_ASSETS
                fi
              fi
            '';
          };
        }
      );
    };
}
