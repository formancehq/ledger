# Settings must be at the very top
Output misc/demo/demo_transactions.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Transactions Demo
# Shows: reference, force transaction (bypass balance check), revert transaction

Sleep 300ms

# ============================================================================
# Setup
# ============================================================================

Type "ledgerctl ledgers create --name demo-tx"
Enter
Sleep 1500ms

# Create a transaction to revert later
Type "ledgerctl transactions create --ledger demo-tx --posting 'world,platform:fees,5000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# Transaction with Reference
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Create a transaction with a reference
Type "ledgerctl transactions create --ledger demo-tx --posting 'world,users:alice,2500,USD/2' --reference 'payment-001'"
Enter
Sleep 2s

# Try to create another transaction with the same reference (will fail)
Type "ledgerctl transactions create --ledger demo-tx --posting 'world,users:bob,1000,USD/2' --reference 'payment-001'"
Enter
Sleep 2s

# ============================================================================
# Force Transaction (bypass balance check)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Try to send from an account with no balance (will fail)
Type "ledgerctl transactions create --ledger demo-tx --posting 'users:broke,users:rich,1000,USD/2'"
Enter
Sleep 2s

# Now use --force to bypass balance check
Type "ledgerctl transactions create --ledger demo-tx --posting 'users:broke,users:rich,1000,USD/2' --force"
Enter
Sleep 2s

# Show the negative balance
Type "ledgerctl accounts get users:broke --ledger demo-tx"
Enter
Sleep 2s

# ============================================================================
# Revert Transaction
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show balance before revert
Type "ledgerctl accounts get platform:fees --ledger demo-tx"
Enter
Sleep 1500ms

# Revert transaction 1 (the platform fees)
Type "ledgerctl transactions revert 1 --ledger demo-tx -y"
Enter
Sleep 2s

# Show balance after revert (should be back to 0)
Type "ledgerctl accounts get platform:fees --ledger demo-tx"
Enter
Sleep 1500ms

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-tx -y"
Enter
Sleep 2s
