{
  description = "A Nix-flake-based Go 1.26 development environment";

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
          # Go-based codegen/lint tools (mockgen, protoc-gen-go/-grpc/-vtproto,
          # golangci-lint, gomarkdoc, ginkgo, controller-gen, setup-envtest) are
          # NOT provided here: they are declared as `tool` directives in go.mod
          # (root) and misc/operator/go.mod and invoked via `go tool <name>`,
          # so their versions are pinned alongside the code they generate.
          stablePackages = with pkgs; [
            go_1_26
            ffmpeg
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
            just
          ];
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
              # It is declared as a `tool` in misc/operator/go.mod and invoked via
              # `go tool` from that module; the `|| true` keeps a build/network
              # failure from breaking `nix develop`.
              if [ -z "$KUBEBUILDER_ASSETS" ]; then
                KUBEBUILDER_ASSETS="$( (cd misc/operator && go tool setup-envtest use -p path) 2>/dev/null || true)"
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
