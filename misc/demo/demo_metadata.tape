# Settings must be at the very top
Output misc/demo/demo_metadata.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Metadata Demo
# Shows: account metadata (set + delete), transaction metadata (set + delete)

Sleep 300ms

# ============================================================================
# Setup
# ============================================================================

Type "ledgerctl ledgers create --name demo-meta"
Enter
Sleep 1500ms

# Create a transaction so we have accounts and a transaction to work with
Type "ledgerctl transactions create --ledger demo-meta --posting 'world,merchants:coffee-shop,5000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# Account Metadata (set + delete)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Set metadata on account
Type "ledgerctl accounts set-metadata merchants:coffee-shop --ledger demo-meta -m 'business=retail' -m 'country=FR'"
Enter
Sleep 1500ms

Type "ledgerctl accounts get merchants:coffee-shop --ledger demo-meta"
Enter
Sleep 1500ms

# Delete metadata from account
Type "ledgerctl accounts delete-metadata merchants:coffee-shop country --ledger demo-meta -y"
Enter
Sleep 1500ms

Type "ledgerctl accounts get merchants:coffee-shop --ledger demo-meta"
Enter
Sleep 1500ms

# ============================================================================
# Transaction Metadata (set + delete)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Set metadata on transaction
Type "ledgerctl transactions set-metadata 1 --ledger demo-meta -m 'reviewed=true' -m 'reviewer=admin'"
Enter
Sleep 1500ms

Type "ledgerctl transactions get 1 --ledger demo-meta"
Enter
Sleep 1500ms

# Delete metadata from transaction
Type "ledgerctl transactions delete-metadata 1 reviewer --ledger demo-meta -y"
Enter
Sleep 1500ms

Type "ledgerctl transactions get 1 --ledger demo-meta"
Enter
Sleep 1500ms

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-meta -y"
Enter
Sleep 2s
