# CLI Demo Generation

This directory contains VHS tape files for generating animated GIF demos of the `ledgerctl` CLI.

## Prerequisites

### Install VHS

VHS is a CLI tool from Charmbracelet that generates terminal GIFs from tape files.

```bash
# macOS (Homebrew)
brew install vhs

# Linux (with Go installed)
go install github.com/charmbracelet/vhs@latest

# Or download from releases
# https://github.com/charmbracelet/vhs/releases
```

VHS also requires `ffmpeg`:

```bash
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt install ffmpeg
```

### Build the CLI

```bash
# From project root
just build-client

# Or manually
go build -o build/ledgerctl ./cmd/ledgerctl
```

## Available Demos

| Demo | File | Description |
|------|------|-------------|
| **Getting Started** | `demo_getting_started.tape` | Create a ledger, interactive transaction wizard, list transactions |
| **Numscript** | `demo_numscript.tape` | Payment with fees, escrow with dynamic accounts (`@escrow:$order_id`) |
| **Transactions** | `demo_transactions.tape` | Force transactions (bypass balance check), revert transactions |
| **Metadata** | `demo_metadata.tape` | Set and delete metadata on accounts and transactions |
| **Operations** | `demo_operations.tape` | Cluster status, store integrity check, store backup |
| **Audit** | `demo_audit.tape` | Audit log listing, failures-only filter, ledger filter |
| **Signing** | `demo_signing.tape` | Generate keypair, bootstrap key registration, signed requests, key management |


Each demo is self-contained: it creates its own ledger, runs the scenario, and cleans up.

## Generating Demos

### Generate all demos

```bash
just generate-demo
```

### Generate a single demo

```bash
just generate-demo-only demo_numscript
```

This will:
1. Start a single-node ledger server in the background (using a temporary directory)
2. Wait for the server to be ready (via gRPC health check)
3. Run VHS to generate the demo GIF(s)
4. Stop the server and clean up temporary files

## Customizing

Edit any `.tape` file to customize:

- **Output**: Change filename and format (gif, mp4, webm)
- **Theme**: Try "Dracula", "Nord", "Catppuccin Mocha", etc.
- **Speed**: Adjust `TypingSpeed` and `PlaybackSpeed`
- **Size**: Modify `Width` and `Height`
- **Commands**: Add or remove demonstration steps

### VHS Commands

```tape
Type "command"           # Type text
Enter                    # Press Enter
Sleep 1s                 # Wait duration
Ctrl+C                   # Key combinations
Backspace 5              # Backspace N times
Hide                     # Hide subsequent commands
Show                     # Show commands again
```

## Troubleshooting

### "command not found: ledgerctl"

Ensure `ledgerctl` is in your PATH:

```bash
export PATH="$PATH:$(pwd)/../../build"
```

### "connection refused"

When using `just generate-demo`, the server is started automatically. If running VHS manually, ensure a ledger server is running on `localhost:8888`.

### Fonts not rendering correctly

Install a Nerd Font or specify a system font:

```tape
Set FontFamily "Monaco"  # macOS
Set FontFamily "Consolas"  # Windows
```
