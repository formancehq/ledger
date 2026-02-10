# Settings must be at the very top
Output misc/demo/demo_numscript.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Numscript Demo
# Shows: payment with fees (interactive variables), escrow with dynamic account

Sleep 300ms

# ============================================================================
# Setup
# ============================================================================

Type "ledgerctl ledgers create --name demo-numscript"
Enter
Sleep 1500ms

# Fund the main account for later use
Type "ledgerctl transactions create --ledger demo-numscript --posting 'world,bank:main,100000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# Numscript with Interactive Variables
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show Numscript file briefly
Type "cat misc/numscript/examples/payment_with_fees.num"
Enter
Sleep 2s

# Execute with interactive variable input
Type "ledgerctl transactions create --ledger demo-numscript --script misc/numscript/examples/payment_with_fees.num"
Enter
Sleep 1500ms

# Enter variables (amount, buyer, platform, seller)
Type "USD/2 5000"
Enter
Sleep 600ms
Type "bank:main"
Enter
Sleep 600ms
Type "platform:fees"
Enter
Sleep 600ms
Type "merchants:coffee-shop"
Enter
Sleep 1500ms

# ============================================================================
# Escrow with Dynamic Account
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show escrow script
Type "cat misc/numscript/examples/escrow_funding.num"
Enter
Sleep 2s

# Fund buyer account
Type "ledgerctl transactions create --ledger demo-numscript --posting 'world,users:alice,25000,USD/2'"
Enter
Sleep 1500ms

# Create escrow with dynamic account @escrow:$order_id
Type "ledgerctl transactions create --ledger demo-numscript --script misc/numscript/examples/escrow_funding.num --var 'buyer=users:alice' --var 'order_id=ORD-2024-001' --var 'amount=USD/2 15000'"
Enter
Sleep 2s

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-numscript -y"
Enter
Sleep 2s
