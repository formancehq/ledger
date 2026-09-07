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
          # Backport golangci-lint's cross-worktree cache fix without upgrading
          # its analyzer set. Upstream commits: 371655d60f85 and 4ca5387e2531.
          golangciLint = pkgs-unstable.golangci-lint.overrideAttrs (_finalAttrs: previousAttrs: {
            patches = (previousAttrs.patches or [ ]) ++ [
              ./misc/devenv/patches/golangci-lint-2.12.2-cross-worktree-cache.patch
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
