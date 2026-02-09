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
go build -o build/ledgerctl ./cmd/client
```

### Start the Server

The demo requires a running ledger server:

```bash
# Option 1: Local development server
just dev

# Option 2: Docker
docker compose up -d

# Option 3: Manual
go run . --node-id 1 --bind-addr localhost:7777 --http-port 9000 --grpc-port 8888
```

## Generating the Demo GIF

### Using just (recommended)

```bash
# Default: localhost:8888 with insecure mode
just generate-demo

# Custom server address (insecure)
just generate-demo myserver.example.com:8888 true

# Secure connection (TLS)
just generate-demo myserver.example.com:443 false

# Simple demo variant
just generate-demo-simple
just generate-demo-simple myserver:443 false
```

### Manual generation

```bash
# Navigate to demo directory
cd misc/demo

# Ensure ledgerctl is in PATH
export PATH="$PATH:$(pwd)/../../build"

# Generate the GIF (default server: localhost:8888)
vhs demo.tape

# With custom server
SERVER=myserver.example.com:8888 INSECURE=true vhs demo.tape

# With TLS (secure connection)
SERVER=myserver.example.com:443 INSECURE=false vhs demo.tape

# Output: demo.gif
```

## Customizing the Demo

Edit `demo.tape` to customize:

- **Output**: Change filename and format (gif, mp4, webm)
- **Theme**: Try "Dracula", "Nord", "Catppuccin Mocha", etc.
- **Speed**: Adjust `TypingSpeed` and `PlaybackSpeed`
- **Size**: Modify `Width` and `Height`
- **Commands**: Add or remove demonstration steps

### Available Settings

```tape
# Environment variables
Env SERVER "localhost:8888"  # Server address (can be overridden)

# Output settings
Output demo.gif          # Output filename
Set Width 1200           # Terminal width in pixels
Set Height 800           # Terminal height in pixels

# Appearance
Set FontSize 14          # Font size
Set FontFamily "JetBrains Mono"
Set Theme "Catppuccin Mocha"
Set Padding 20           # Padding around terminal

# Timing
Set TypingSpeed 50ms     # Time between keystrokes
Set PlaybackSpeed 0.75   # Playback speed multiplier
Set Framerate 30         # Output framerate
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER` | `localhost:8888` | gRPC server address for ledgerctl |
| `INSECURE` | `true` | Use insecure connection (no TLS) |

Override from command line:
```bash
SERVER=myserver:8888 INSECURE=true vhs demo.tape
```

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

## Alternative: Simple Tape

For a quicker demo without Numscript files:

```tape
Output simple-demo.gif
Set FontSize 16
Set Width 1000
Set Height 600
Set Theme "Dracula"
Set TypingSpeed 40ms

Type "ledgerctl ledgers create --name demo"
Enter
Sleep 2s

Type "ledgerctl tx create --ledger demo --posting 'world,bank,1000,USD/2'"
Enter
Sleep 2s

Type "ledgerctl accounts get bank --ledger demo"
Enter
Sleep 2s
```

## Troubleshooting

### "command not found: ledgerctl"

Ensure `ledgerctl` is in your PATH:

```bash
export PATH="$PATH:$(pwd)/../../build"
# Or use absolute path in the tape file
```

### "connection refused"

The ledger server must be running on `localhost:8888` (or configure `--server` flag).

### Fonts not rendering correctly

Install a Nerd Font or specify a system font:

```tape
Set FontFamily "Monaco"  # macOS
Set FontFamily "Consolas"  # Windows
```

## Output Formats

VHS supports multiple output formats:

```tape
Output demo.gif      # Animated GIF (default)
Output demo.mp4      # MP4 video
Output demo.webm     # WebM video
Output demo.png      # PNG frames (creates directory)
```
