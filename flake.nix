{
  description = "A Nix-flake-based Go 1.27 development environment";

  inputs = {
    # Keep golangci-lint 2.15.0; stable currently provides 2.12.2.
    go-tooling = {
      url = "github:NixOS/nixpkgs/8cec5ef1131078ea76689e84c362b0ed6a1fb6a1";
      flake = false;
    };
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";
    nixpkgs-unstable.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";

    nur = {
      url = "github:nix-community/NUR";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, go-tooling, nixpkgs, nixpkgs-unstable, nur }:
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
          # Use upstream Go so CI can reuse the stable nixpkgs binary cache.
          go = pkgs.go_1_27;
          # Preserve upstream dependencies so the public binary cache matches.
          # The upstream package set no longer supports Intel macOS.
          golangci-lint =
            if system == "x86_64-darwin" then
              pkgs.callPackage "${go-tooling}/pkgs/by-name/go/golangci-lint/package.nix" { }
            else
              (import go-tooling { inherit system; }).golangci-lint;

          # Mock generation must understand the root module Go version.
          buildGoModule = pkgs.buildGo127Module;

          # Tools invoked by CI jobs. Every CI job downloads this closure on a
          # cold Nix cache, so a tool belongs here only if a workflow step,
          # or a recipe/script a workflow step runs, executes it.
          ciPackages = (with pkgs; [
            go
            go-jsonnet
            jsonnet-bundler
            jq
            kubernetes-helm
            python314
          ]) ++ (with pkgs-unstable; [
            # scripts/checkprotocolversion classifies service contract breaks.
            buf
            protobuf_34
            (mockgen.override { inherit buildGoModule; })
            protoc-gen-go
            protoc-gen-go-grpc
            protoc-gen-go-vtproto
            just
          ]) ++ [
            golangci-lint
            pkgs.nur.repos.goreleaser.goreleaser-pro
          ];

          # Everyday local tooling on top of the CI set: pre-commit hooks
          # (.envrc), the Ginkgo CLI, grpcurl, Jira publication and operator
          # envtest.
          localPackages = (with pkgs; [
            acli
            ginkgo
            grpcurl
            trufflehog
            uv
          ]) ++ (with pkgs-unstable; [
            setup-envtest
          ]);

          # Heavy or rarely used tools get their own shell so the default and
          # CI shells do not download them on every cold start.
          mkTaskShell = packages: pkgs.mkShell {
            packages = ciPackages ++ packages;
            GOTOOLCHAIN = "local";
          };
        in
        {
          ci = pkgs.mkShell {
            packages = ciPackages;
            GOTOOLCHAIN = "local";
          };

          # `just generate-demo`: vhs wraps ffmpeg, ttyd and, on Linux, chromium.
          demo = mkTaskShell (with pkgs; [ grpcurl vhs ]);

          # `just test-sdk-bigint` and tests/sdk contract probes.
          sdk = mkTaskShell [
            pkgs.nodejs_22
            self.packages.${system}.speakeasy
          ];

          # tests/perf k6 load tests and benchstat comparisons.
          perf = mkTaskShell [ pkgs.k6 pkgs-unstable.goperf ];

          # API-server admission regressions for the operator, without Docker.
          operator-tests = mkTaskShell [ pkgs-unstable.setup-envtest ];

          # Three-voter rc.1 binary upgrade in an isolated kind cluster.
          upgrade = mkTaskShell (with pkgs; [ kyverno-chainsaw kind kubectl docker-client gh ]);

          default = pkgs.mkShell {
            packages = ciPackages ++ localPackages;
            GOTOOLCHAIN = "local";

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
