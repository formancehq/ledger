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
      devShells = forEachSupportedSystem ({ pkgs, pkgs-unstable, system }:
        let
          # Use upstream's cross-worktree cache fix until the locked channel
          # provides this release. Keep the rest of the toolchain unchanged.
          golangciLint = pkgs-unstable.golangci-lint.overrideAttrs (_finalAttrs: _previousAttrs: {
            version = "2.13.2";
            src = pkgs-unstable.fetchFromGitHub {
              owner = "golangci";
              repo = "golangci-lint";
              tag = "v2.13.2";
              hash = "sha256-RbWKPIG+UK82S9W9tp/CciZ669vudh95VOfHfdQWx3M=";
            };
            vendorHash = "sha256-R83GeyfuZ+w30jZqFGYi0yua8E1Ey2q7/OlVmw8zDCg=";
            ldflags = [
              "-s"
              "-w"
              "-X main.version=2.13.2"
              "-X main.commit=v2.13.2"
              "-X main.date=1970-01-01T00:00:00Z"
            ];
          });
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
            mockgen
            protoc-gen-go
            protoc-gen-go-grpc
            protoc-gen-go-vtproto
            setup-envtest
            just
          ] ++ [ golangciLint ];
          otherPackages = [
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
