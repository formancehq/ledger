# Settings must be at the very top
Output misc/demo/demo_periods.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Periods Demo
# Shows: close period, list periods

Sleep 300ms

# ============================================================================
# Setup - create a ledger with some transactions
# ============================================================================

Type "ledgerctl ledgers create --name demo-periods"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-periods --posting 'world,bank:main,50000,USD/2'"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-periods --posting 'bank:main,user:alice,1500,USD/2'"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-periods --posting 'bank:main,user:bob,2000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# List Periods (initial state - one OPEN period)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl periods list"
Enter
Sleep 3s

# ============================================================================
# Close the current period
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl periods close"
Enter
Sleep 3s

# ============================================================================
# List Periods (shows CLOSED + new OPEN period)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl periods list"
Enter
Sleep 4s

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-periods -y"
Enter
Sleep 2s
