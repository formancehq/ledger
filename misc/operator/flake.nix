{
  description = "Ledger Operator development environment";

  inputs = {
    # Keep application tooling pinned; update only the compiler and its linter.
    go-tooling = {
      url = "github:NixOS/nixpkgs/8cec5ef1131078ea76689e84c362b0ed6a1fb6a1";
      flake = false;
    };
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2511";
    nixpkgs-unstable.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";
  };

  outputs = { self, go-tooling, nixpkgs, nixpkgs-unstable }:
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
          # Import only Go recipes so unrelated tools and all four platforms stay pinned.
          go126 = pkgs.callPackage "${go-tooling}/pkgs/development/compilers/go/1.26.nix" {
            buildGo126Module = build126;
          };
          go127 = pkgs.callPackage "${go-tooling}/pkgs/development/compilers/go/1.27.nix" {
            buildGo127Module = build127;
          };
          build126 = pkgs.buildGoModule.override { go = go126; };
          build127 = pkgs.buildGoModule.override { go = go127; };
          go = go126;
          golangci-lint = pkgs.callPackage "${go-tooling}/pkgs/by-name/go/golangci-lint/package.nix" {
            buildGo127Module = build127;
          };

          stablePackages = with pkgs; [
            go
            gotools
            just
            helm-docs
            kubernetes-helm
            kind
          ];
        in
        {
          default = pkgs.mkShell {
            packages = stablePackages ++ [ golangci-lint ];
            GOTOOLCHAIN = "local";

            shellHook = ''
              export GOROOT="${go}/share/go"
              export PATH="$(go env GOPATH)/bin:$PATH"
            '';
          };
        }
      );
    };
}
