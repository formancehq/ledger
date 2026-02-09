# Settings must be at the very top
Output misc/demo/demo.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Ledger v3 CLI Demo
# Environment variables: SERVER, INSECURE (inherited from shell)

Sleep 300ms

# ============================================================================
# PART 1: Setup
# ============================================================================

Type "ledgerctl ledgers create --name demo"
Enter
Sleep 1500ms

# ============================================================================
# PART 2: Interactive Wizard - Simple Posting
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
# PART 3: Numscript with Interactive Variables
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show Numscript file briefly
Type "cat misc/numscript/examples/payment_with_fees.num"
Enter
Sleep 2s

# Execute with interactive variable input
Type "ledgerctl transactions create --ledger demo --script misc/numscript/examples/payment_with_fees.num"
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
# PART 4: Escrow with Dynamic Account
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show escrow script
Type "cat misc/numscript/examples/escrow_funding.num"
Enter
Sleep 2s

# Fund buyer account
Type "ledgerctl transactions create --ledger demo --posting 'world,users:alice,25000,USD/2'"
Enter
Sleep 1500ms

# Create escrow with dynamic account @escrow:$order_id
Type "ledgerctl transactions create --ledger demo --script misc/numscript/examples/escrow_funding.num --var 'buyer=users:alice' --var 'order_id=ORD-2024-001' --var 'amount=USD/2 15000'"
Enter
Sleep 2s

# ============================================================================
# PART 5: Force Transaction (bypass balance check)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Try to send from an account with no balance (will fail)
Type "# Trying to send from an empty account (insufficient funds)..."
Enter
Sleep 800ms
Type "ledgerctl transactions create --ledger demo --posting 'users:broke,users:rich,1000,USD/2'"
Enter
Sleep 2s

# Now use --force to bypass balance check
Type "# Using --force to bypass balance check..."
Enter
Sleep 800ms
Type "ledgerctl transactions create --ledger demo --posting 'users:broke,users:rich,1000,USD/2' --force"
Enter
Sleep 2s

# Show the negative balance
Type "ledgerctl accounts get users:broke --ledger demo"
Enter
Sleep 2s

# ============================================================================
# PART 6: Account Metadata (set + delete)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Set metadata on account
Type "ledgerctl accounts set-metadata merchants:coffee-shop --ledger demo -m 'business=retail' -m 'country=FR'"
Enter
Sleep 1500ms

Type "ledgerctl accounts get merchants:coffee-shop --ledger demo"
Enter
Sleep 1500ms

# Delete metadata from account
Type "ledgerctl accounts delete-metadata merchants:coffee-shop country --ledger demo -y"
Enter
Sleep 1500ms

Type "ledgerctl accounts get merchants:coffee-shop --ledger demo"
Enter
Sleep 1500ms

# ============================================================================
# PART 7: Transaction Metadata (set + delete)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Set metadata on transaction
Type "ledgerctl transactions set-metadata 1 --ledger demo -m 'reviewed=true' -m 'reviewer=admin'"
Enter
Sleep 1500ms

Type "ledgerctl transactions get 1 --ledger demo"
Enter
Sleep 1500ms

# Delete metadata from transaction
Type "ledgerctl transactions delete-metadata 1 reviewer --ledger demo -y"
Enter
Sleep 1500ms

Type "ledgerctl transactions get 1 --ledger demo"
Enter
Sleep 1500ms

# ============================================================================
# PART 8: Revert Transaction
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show balance before revert
Type "ledgerctl accounts get platform:fees --ledger demo"
Enter
Sleep 1500ms

# Revert transaction 2 (the payment with fees)
Type "ledgerctl transactions revert 2 --ledger demo -y"
Enter
Sleep 2s

# Show balance after revert (should be back to 0)
Type "ledgerctl accounts get platform:fees --ledger demo"
Enter
Sleep 1500ms

# ============================================================================
# PART 9: List All Transactions
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl transactions list --ledger demo --all"
Enter
Sleep 3s

# ============================================================================
# PART 10: Cluster Status
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show cluster status from leader (default)
Type "ledgerctl cluster status"
Enter
Sleep 3s

# ============================================================================
# PART 11: Cleanup - Delete Ledger
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo -y"
Enter
Sleep 2s
