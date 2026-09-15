{
  description = "A Nix-flake-based Go 1.27 development environment";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";
    nixpkgs-unstable.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";
    nur = {
      url = "github:nix-community/NUR";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, nur, rust-overlay }:
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
              overlays = [ nur.overlays.default rust-overlay.overlays.default ];
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
      # The Ledger v3 fctl plugin builds a portable WebAssembly component. Its
      # authoring toolchain is not published in nixpkgs, so the pinned
      # definitions are carried in-tree rather than pulled from the private
      # fctl flake, which Ledger CI cannot reach. Keep it out of the default
      # shell: these are Rust builds, and making every Go CI job fetch crates
      # turns a crates.io rate limit into an unrelated red build.
      packages = forEachSupportedSystem ({ pkgs, ... }:
        let
          componentTools = pkgs.callPackage ./nix/fctl-component-tools.nix { };
        in
        {
          inherit (componentTools) componentize-go wasi-virt wasm-tools;
          wasm-opt = pkgs.binaryen;
        }
      );

      devShells = forEachSupportedSystem ({ pkgs, pkgs-unstable, system }:
        let
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
            golangci-lint
            setup-envtest
            just
          ];
          otherPackages = [
            pkgs.nur.repos.goreleaser.goreleaser-pro
          ];
        in
        {
          default = pkgs.mkShell {
            packages = stablePackages ++ unstablePackages ++ otherPackages;
            # The fctl repository is private and the Ledger CI token is scoped
            # to this repository. Use the exact, lock-verified SDK snapshot
            # committed with the plugin so clean CI needs no cross-repository
            # credential or mutable network lookup.
            FCTL_SDK_ROOT = ./plugins/fctl/ledger-v3/sdk/fctl-v2-poc;

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
