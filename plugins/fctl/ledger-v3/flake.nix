{
  description = "Ledger v3 fctl portable-component build tools";

  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";

  # Keep the authoring toolchain pinned without embedding a workstation path.
  # Go SDK resolution is separately content-addressed by fctl-sdk.lock.json and
  # the ephemeral workspace created by scripts/with-fctl-sdk.sh.
  inputs.fctl.url = "github:Dav-14/fctl-v2-poc/545521bfa222250af6b4419b194c7967cded0379";

  outputs = { nixpkgs, fctl, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));
    in {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
		  inputsFrom = [ fctl.devShells.${pkgs.system}.default ];
          packages = [ pkgs.binaryen ];
        };
      });
    };
}
