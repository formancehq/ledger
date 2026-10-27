{
  description = "Ledger Operator development environment";

  inputs = {
    # Keep golangci-lint 2.15.0; stable currently provides 2.12.2.
    go-tooling = {
      url = "github:NixOS/nixpkgs/8cec5ef1131078ea76689e84c362b0ed6a1fb6a1";
      flake = false;
    };
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";
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
          # Use upstream Go to reuse the binary cache.
          go = pkgs.go_1_27;
          # Preserve upstream dependencies so the public binary cache matches.
          # The upstream package set no longer supports Intel macOS.
          golangci-lint =
            if system == "x86_64-darwin" then
              pkgs.callPackage "${go-tooling}/pkgs/by-name/go/golangci-lint/package.nix" { }
            else
              (import go-tooling { inherit system; }).golangci-lint;

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
