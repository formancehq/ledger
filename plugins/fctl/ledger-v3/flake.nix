{
  description = "Ledger v3 fctl portable-component build tools";

  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2605";

  # Keep the authoring toolchain pinned without embedding a workstation path.
  # Go SDK resolution is separately content-addressed by fctl-sdk.lock.json and
  # the ephemeral workspace created by scripts/with-fctl-sdk.sh.
  #
  # This pin supplies only devShells.default — the Go toolchain plus
  # componentize-go, wasi-virt and wasm-tools. It is the newest commit reachable
  # on the canonical repository; the SDK commit named by fctl-sdk.lock.json is
  # not published there yet. Both commits define byte-identical
  # devShells.default and identical componentize-go/wasi-virt/wasm-tools
  # derivations, so the authoring toolchain is the same either way. Move this
  # pin onto the locked SDK commit once that commit is published.
  inputs.fctl.url = "github:formancehq/fctl-v2-poc/01fccf28233fe51edfa71f21a8cde3dabfca3bcb";

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
