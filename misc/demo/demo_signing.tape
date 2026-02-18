# Settings must be at the very top
Output misc/demo/demo_signing.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Request Signing Demo
# Shows: generate keypair, bootstrap, signed requests, multi-key management

Sleep 300ms

# ============================================================================
# Step 1: Generate an Ed25519 keypair
# ============================================================================

Type `echo "--- Step 1: Generate an Ed25519 keypair ---"`
Enter
Sleep 800ms

Type "ledgerctl signing generate-key /tmp/demo-keys"
Enter
Sleep 2500ms

# Show the public key
Type "cat /tmp/demo-keys/pubkey.hex"
Enter
Sleep 2s

# ============================================================================
# Step 2: Bootstrap — register the first key (no signature needed)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type `echo "--- Step 2: Bootstrap - register first key (unsigned) ---"`
Enter
Sleep 800ms

Type "ledgerctl signing register-key --key-id default --public-key-file /tmp/demo-keys/pubkey.hex"
Enter
Sleep 2500ms

# ============================================================================
# Step 3: Sign write operations with the key
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type `echo "--- Step 3: Signed write operations ---"`
Enter
Sleep 800ms

# Create a ledger (signed)
Type "ledgerctl --signing-key /tmp/demo-keys/seed.hex ledgers create --name demo-signing"
Enter
Sleep 2s

# Create a transaction (signed)
Type "ledgerctl --signing-key /tmp/demo-keys/seed.hex transactions create --ledger demo-signing --posting 'world,bank,10000,USD/2'"
Enter
Sleep 2500ms

# ============================================================================
# Step 4: Register an additional key (must be signed by admin)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type `echo "--- Step 4: Register a second key (signed by admin) ---"`
Enter
Sleep 800ms

# Generate a second keypair silently
Hide
Type "ledgerctl signing generate-key /tmp/demo-keys-ops"
Enter
Sleep 1500ms
Show

Type "ledgerctl signing register-key --key-id ops --public-key-file /tmp/demo-keys-ops/pubkey.hex --signing-key /tmp/demo-keys/seed.hex"
Enter
Sleep 2500ms

# Use the ops key
Type "ledgerctl --signing-key /tmp/demo-keys-ops/seed.hex --signing-key-id ops transactions create --ledger demo-signing --posting 'bank,users:alice,500,USD/2'"
Enter
Sleep 2500ms

# ============================================================================
# Step 5: Revoke the ops key
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type `echo "--- Step 5: Revoke a key & enforce mandatory signing ---"`
Enter
Sleep 800ms

Type "ledgerctl signing revoke-key --key-id ops --signing-key /tmp/demo-keys/seed.hex"
Enter
Sleep 2500ms

# Enforce mandatory signatures
Type "ledgerctl signing require true --signing-key /tmp/demo-keys/seed.hex"
Enter
Sleep 2500ms

# ============================================================================
# Cleanup
# ============================================================================

Hide
Type "ledgerctl signing require false --signing-key /tmp/demo-keys/seed.hex"
Enter
Sleep 1500ms
Show

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl --signing-key /tmp/demo-keys/seed.hex ledgers delete demo-signing -y"
Enter
Sleep 2s

Hide
Type "rm -rf /tmp/demo-keys /tmp/demo-keys-ops"
Enter
Sleep 200ms
Show
