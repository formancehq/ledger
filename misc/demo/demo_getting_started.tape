# Settings must be at the very top
Output misc/demo/demo_getting_started.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Getting Started Demo
# Shows: create ledger, interactive wizard, list transactions

Sleep 300ms

# ============================================================================
# Create ledger
# ============================================================================

Type "ledgerctl ledgers create --name demo"
Enter
Sleep 1500ms

# ============================================================================
# Interactive Wizard - Simple Posting
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Start interactive transaction creation (wizard mode)
Type "ledgerctl transactions create --ledger demo"
Enter
Sleep 1200ms

# Select "Simple postings" (first option)
Enter
Sleep 800ms

# Source, Destination, Amount, Asset
Type "world"
Enter
Sleep 400ms
Type "bank:main"
Enter
Sleep 400ms
Type "100000"
Enter
Sleep 400ms
Type "USD/2"
Enter
Sleep 800ms

# Add another? No
Type "n"
Enter
Sleep 1500ms

# ============================================================================
# List All Transactions
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl transactions list --ledger demo --all"
Enter
Sleep 3s

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo -y"
Enter
Sleep 2s
