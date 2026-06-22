# Settings must be at the very top
Output misc/demo/demo_chapters.gif
Set FontSize 14
Set Width 1200
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set Padding 20
Set Framerate 30
Set PlaybackSpeed 0.8
Set Shell zsh

# Chapters Demo
# Shows: close chapter, list chapters

Sleep 300ms

# ============================================================================
# Setup - create a ledger with some transactions
# ============================================================================

Type "ledgerctl ledgers create --name demo-chapters"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-chapters --posting 'world,bank:main,50000,USD/2'"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-chapters --posting 'bank:main,user:alice,1500,USD/2'"
Enter
Sleep 1500ms

Type "ledgerctl transactions create --ledger demo-chapters --posting 'bank:main,user:bob,2000,USD/2'"
Enter
Sleep 1500ms

# ============================================================================
# List Chapters (initial state - one OPEN chapter)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl chapters list"
Enter
Sleep 3s

# ============================================================================
# Close the current chapter
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl chapters close"
Enter
Sleep 3s

# ============================================================================
# List Chapters (shows CLOSED + new OPEN chapter)
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl chapters list"
Enter
Sleep 4s

# ============================================================================
# Cleanup
# ============================================================================

Type "clear"
Enter
Sleep 200ms

Type "ledgerctl ledgers delete demo-chapters -y"
Enter
Sleep 2s
