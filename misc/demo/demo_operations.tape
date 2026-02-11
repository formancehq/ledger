# Settings must be at the very top
Output misc/demo/demo_operations.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Operations Demo
# Shows: cluster status, store integrity check, store backup

Sleep 300ms

# ============================================================================
# Setup - create a ledger with some data for the integrity check
# ============================================================================

Type "ledgerctl ledgers create --name demo-ops"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-ops --posting 'world,bank:main,10000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# Cluster Status
# ============================================================================

Type "clear"
Enter
Sleep 200ms

# Show cluster status from leader (default)
Type "ledgerctl cluster status"
Enter
Sleep 3s

# ============================================================================
# Store Integrity Check
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl store check"
Enter
Sleep 3s

# ============================================================================
# Store Backup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl store backup -o /tmp/demo-backup.tar"
Enter
Sleep 4s

# Show the backup file
Type "ls -lh /tmp/demo-backup.tar"
Enter
Sleep 2s

# Cleanup backup file
Hide
Type "rm -f /tmp/demo-backup.tar"
Enter
Sleep 200ms
Show

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-ops -y"
Enter
Sleep 2s
