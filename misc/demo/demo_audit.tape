# Settings must be at the very top
Output misc/demo/demo_audit.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Audit Log Demo
# Shows: audit entries for successful and failed transactions

Sleep 300ms

# ============================================================================
# Setup - enable audit logging then create a ledger with data
# ============================================================================

Type "ledgerctl audit enable"
Enter
Sleep 1500ms

Type "ledgerctl ledgers create --name demo-audit"
Enter
Sleep 1500ms

# Create a successful transaction
Type "ledgerctl transactions create --ledger demo-audit --posting 'world,bank:main,10000,USD/2'"
Enter
Sleep 1500ms

# Create a transaction that fails (insufficient funds)
Type "ledgerctl transactions create --ledger demo-audit --posting 'empty:account,bank:main,99999,USD/2'"
Enter
Sleep 2s

# ============================================================================
# List all audit entries
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl audit list"
Enter
Sleep 3s

# ============================================================================
# List only failures
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl audit list --failures-only"
Enter
Sleep 3s

# ============================================================================
# Filter by ledger
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl audit list --ledger demo-audit"
Enter
Sleep 3s

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-audit -y"
Enter
Sleep 2s
