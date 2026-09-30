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
      # RocksDB POC (docs/drafts/rocksdb-poc.md): pin the exact RocksDB
      # version targeted by grocksdb v1.11.0 and shipped by Alpine, so the
      # Nix shell and the Docker build link the same engine. nixpkgs is
      # still at 10.10.1, hence the override.
      rocksdbOverlay = final: prev: {
        rocksdb = prev.rocksdb.overrideAttrs (finalAttrs: _: {
          version = "11.0.4";
          src = final.fetchFromGitHub {
            owner = "facebook";
            repo = "rocksdb";
            tag = "v${finalAttrs.version}";
            hash = "sha256-j7+IXVyQGDNE2xHGn4iaZv5v2ez/jnOBU0qwhr08ATU=";
          };
        });
      };

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
              overlays = [ nur.overlays.default rocksdbOverlay ];
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
          # cgo inputs for the RocksDB POC (misc/rocksdb-poc). Exposed as
          # dedicated variables so regular builds keep CGO_ENABLED=0.
          rocksdbPackages = with pkgs; [ rocksdb pkg-config ];
          rocksdbLibDirs = with pkgs; [ rocksdb zstd lz4 snappy zlib bzip2 ];
          rocksdbLdflags = pkgs.lib.concatMapStringsSep " " (p: "-L${pkgs.lib.getLib p}/lib") rocksdbLibDirs;
        in
        {
          default = pkgs.mkShell {
            packages = stablePackages ++ unstablePackages ++ otherPackages ++ rocksdbPackages;

            shellHook = ''
              # RocksDB POC: consumed by `just rocksdb-hello`.
              export ROCKSDB_CGO_CFLAGS="-I${pkgs.lib.getDev pkgs.rocksdb}/include"
              export ROCKSDB_CGO_LDFLAGS="${rocksdbLdflags} -lrocksdb"

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
