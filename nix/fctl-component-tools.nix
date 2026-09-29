{ rust-bin, makeRustPlatform, fetchFromGitHub, pkg-config, bzip2, openssl }:

let
  # Frozen by fctl SDK revision e9b1395f46f3100b381dbe00f5213de28e6df0e1.
  fctlSDKRevision = "e9b1395f46f3100b381dbe00f5213de28e6df0e1";
  rustToolchain = rust-bin.stable."1.91.1".minimal;
  rustPlatform = makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };
  wasmToolsVersion = "1.239.0";
  wasm-tools = rustPlatform.buildRustPackage {
    pname = "wasm-tools";
    version = wasmToolsVersion;
    src = fetchFromGitHub {
      owner = "bytecodealliance";
      repo = "wasm-tools";
      rev = "v${wasmToolsVersion}";
      hash = "sha256-9wJSNC/clO8M7E840i1lRWJQT7AdRQX468swmZ4O1rg=";
    };
    cargoHash = "sha256-xIfTYJMVP47timzquEYEb9M8BHsj83NjgD44lbzgd+Y=";
    cargoBuildFlags = [ "--package" "wasm-tools" ];
    auditable = false;
    doCheck = false;
  };
  componentize-go = rustPlatform.buildRustPackage {
    pname = "componentize-go";
    version = "0.4.1";
    src = fetchFromGitHub {
      owner = "bytecodealliance";
      repo = "componentize-go";
      rev = "v0.4.1";
      hash = "sha256-Kgh17i2vnCAfGaBTNmVafYsG+WJ4OatEdOmrCwrRUs4=";
    };
    cargoHash = "sha256-DoeHrw1+LI3aLhSGMFrUH347nZV2ftykRQ/JUSaERlA=";
    nativeBuildInputs = [ pkg-config ];
    buildInputs = [ bzip2 openssl ];
    checkFlags = [ "--skip" "utils::tests::test_install_go_times_out_on_stalled_server" ];
    auditable = false;
    passthru = { inherit fctlSDKRevision; };
  };
  wasi-virt = rustPlatform.buildRustPackage {
    pname = "wasi-virt";
    version = "0.2.0-448f6df8";
    src = fetchFromGitHub {
      owner = "bytecodealliance";
      repo = "WASI-Virt";
      rev = "448f6df8f688cee5d6995e96b1ffc31f9bf00742";
      hash = "sha256-g6g1gT7cj8U740ew6c+GT3ZVOENU4wqFf10oHni8MOs=";
    };
    cargoHash = "sha256-P39bkgjlqy8/L7iSA7/hQDOfa3WUF+YV3DlcoGZ73jo=";
    patches = [ ./patches/wasi-virt-closed-terminal-stdio.patch ];
    cargoBuildFlags = [ "--package" "wasi-virt" ];
    buildNoDefaultFeatures = true;
    doCheck = false;
    auditable = false;
    passthru = { inherit fctlSDKRevision; };
  };
in
{
  inherit componentize-go wasi-virt wasm-tools;
}
