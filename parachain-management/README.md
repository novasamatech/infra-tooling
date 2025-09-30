# Parachain Onboarding Scripts for Test Networks

A comprehensive toolkit for parachain management in Polkadot/Kusama/Westend test networks.

## 🎯 Key Features

- ✅ **Register new parachains**
- ✅ **Deregister existing parachains** 
- ✅ **Re-register parachains** (deregister → register)
- ✅ **Comprehensive parachain diagnostics**
- ✅ **Automatic issue fixing**
- ✅ **Block production monitoring**
- ✅ **Safe confirmation prompts for critical operations**

## 📦 Installation

```bash
pip install substrate-interface
# or from requirements.txt
pip install -r requirements.txt
```

## 🚀 Main Script - `onboard-parachain.py`

Universal script for all parachain operations in test networks. Handles registration, deregistration, and re-registration with built-in safety checks.

### Basic Usage

```bash
# Check parachain status (read-only)
./onboard-parachain.py wss://relay-rpc.com wss://para-rpc.com

# Register new parachain with activation wait
./onboard-parachain.py wss://relay-rpc.com wss://para-rpc.com --wait

# Re-register existing parachain (useful for pre-deploy)
./onboard-parachain.py wss://relay-rpc.com wss://para-rpc.com --force-reregister --yes

# Deregister parachain only
./onboard-parachain.py wss://relay-rpc.com --deregister-only --para-id 1001 --wait
```

### Complete Options Reference

```bash
# Basic parameters
--seed "0x..."              # Custom sudo seed (default: //Alice)
--lease-periods 100         # Number of lease periods for registration

# Actions
--force-reregister          # Deregister then register (full refresh)
--deregister-only          # Only deregister parachain
--para-id 1001             # Parachain ID (required for --deregister-only)

# Waiting and completion
--wait                     # Wait for operation completion (both reg/dereg)

# Safety and automation
--force-cleanup           # Use force cleanup if deregistration fails (DANGEROUS)
--yes                     # Skip all confirmation prompts (use carefully)
```

### Safety Features

The script includes built-in safety confirmations for destructive operations:

```
🚨 WARNING: PARACHAIN DEREGISTRATION
============================================================
You are about to DEREGISTER parachain 1001
Current status: Parachain
All registered parachains: [1000, 1001, 2000]

This will:
  - Remove the parachain from the relay chain
  - Clear all parachain data and state  
  - Make the parachain ID available for reuse

This action is IRREVERSIBLE!
============================================================
Are you sure you want to deregister this parachain? (y/N):
```

## 🔍 Diagnostic Script - `diagnose-parachain.py`

Comprehensive parachain health checker that identifies issues, monitors block production, and can automatically fix common problems. This script combines diagnostics with collation issue detection and fixing.

### Usage

```bash
# Basic diagnostics
./diagnose-parachain.py wss://relay-rpc.com 1001

# Detailed diagnostics with verbose output
./diagnose-parachain.py wss://relay-rpc.com 1001 --verbose

# Monitor block production for 30 seconds
./diagnose-parachain.py wss://relay-rpc.com 1001 --monitor-blocks

# Full diagnostics with automatic fixes
./diagnose-parachain.py wss://relay-rpc.com 1001 --monitor-blocks --fix --verbose
```

### Available Options

```bash
--verbose              # Show detailed diagnostic information
--monitor-blocks       # Monitor parachain block production for 30 seconds
--fix                 # Automatically attempt to fix detected issues
--seed "0x..."        # Custom sudo seed for fixes (default: //Alice)
```

### What It Checks

- **Registration Status** - Whether parachain is registered on relay chain
- **Lifecycle State** - Current state (Parachain/Parathread/Onboarding/etc)
- **Lease Status** - Active lease periods and validity
- **Validator Assignment** - Number of validator groups available
- **Core Assignment** - Whether parachain is scheduled on validator cores
- **Validation Code** - Presence and validity of parachain runtime code
- **Pending Operations** - Any stuck availability or upgrade operations
- **Code Upgrades** - Future runtime upgrades scheduled
- **Block Production** - Real-time monitoring of parachain head updates (with --monitor-blocks)
- **Block Heights** - Current relay chain and parachain block information

### What It Can Fix

When used with `--fix` flag:
- Forces parachain inclusion in next block
- Triggers validation code updates
- Applies queue actions to process pending operations
- Re-verifies block production after applying fixes

### Sample Output

```
Diagnosing parachain 1001 on wss://relay-rpc.com
============================================================

1. Registration Status:
  ✓ Parachain 1001 is registered

2. Lifecycle Status:  
  ✓ Lifecycle state: Parachain

3. Lease Status:
  ✓ Has lease: Yes

4. Validator Assignment:
  ✓ Number of validator groups: 2

5. Core Assignment:
  ✗ Scheduled on core: No

12. Block Production Monitoring:
  Monitoring parachain head updates for 30 seconds...
  Initial head: 0x00000000000000000000000000000000000000000000000000000000000000...
  ✗ Parachain head did NOT update in 30 seconds

============================================================
❌ CRITICAL ISSUES:
  - Parachain 1001 is NOT scheduled on any core
  - Parachain head not updating: No updates

APPLYING FIXES
============================================================
Using sudo account: 5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY
Attempting to force include parachain 1001...
✓ Queue action forced successfully

Re-checking block production:
✅ Fix successful! Blocks are now being produced!
```

## 📋 Common Usage Scenarios

### 1. Fresh Parachain Registration

```bash
# Step 1: Register with activation wait
./onboard-parachain.py \
    wss://westend-local-relay:9944 \
    wss://collectives-local:9944 \
    --wait

# Step 2: Verify registration and check for issues
./diagnose-parachain.py wss://westend-local-relay:9944 1001 --monitor-blocks --verbose

# Step 3: Fix any detected issues
./diagnose-parachain.py wss://westend-local-relay:9944 1001 --fix
```

### 2. Pre-Deploy Parachain Updates

```bash
# Quick re-registration without prompts (perfect for CI/CD)
./onboard-parachain.py \
    wss://westend-local-relay:9944 \
    wss://collectives-local:9944 \
    --force-reregister \
    --wait \
    --yes

# Verify deployment with block monitoring
./diagnose-parachain.py wss://westend-local-relay:9944 1001 --monitor-blocks
```

### 3. Test Network Cleanup

```bash
# Remove all test parachains
./onboard-parachain.py wss://westend-local-relay:9944 --deregister-only --para-id 1001 --yes
./onboard-parachain.py wss://westend-local-relay:9944 --deregister-only --para-id 2000 --yes  
./onboard-parachain.py wss://westend-local-relay:9944 --deregister-only --para-id 2001 --yes
```

### 4. Troubleshooting Collation Issues

```bash
# When collators show "Collation wasn't advertised to any validator"
./diagnose-parachain.py wss://relay-rpc.com 1001 --monitor-blocks --fix

# Deep analysis with verbose output
./diagnose-parachain.py wss://relay-rpc.com 1001 --verbose --monitor-blocks --fix

# Nuclear option: complete re-registration  
./onboard-parachain.py wss://relay-rpc.com wss://para-rpc.com --force-reregister --wait --yes
```

## 🚨 Common Problems & Solutions

### Problem: "Collation wasn't advertised to any validator"

**Root Causes:**
1. Network connectivity issues between collators and validators
2. Incorrect relay chain spec on collators  
3. Validators not assigned to parachain cores
4. Parachain validation code issues

**Solution Steps:**
```bash
# 1. Quick diagnostic with fix
./diagnose-parachain.py wss://relay-rpc.com 1001 --monitor-blocks --fix

# 2. Deep analysis if issue persists
./diagnose-parachain.py wss://relay-rpc.com 1001 --verbose --monitor-blocks

# 3. Last resort - full re-registration
./onboard-parachain.py wss://relay-rpc.com wss://para-rpc.com --force-reregister --wait
```

### Problem: Parachain Stuck in "Onboarding" State

**Solution:**
```bash
./diagnose-parachain.py wss://relay-rpc.com 1001 --fix
```

### Problem: No Block Finalization

**Check:**
1. Minimum 2 validators in network
2. Network connectivity between nodes
3. Correct relay chain specification on collators

**Commands:**
```bash
# Verify validator count and parachain assignment
./diagnose-parachain.py wss://relay-rpc.com 1001 --verbose

# Monitor block production and attempt fixes
./diagnose-parachain.py wss://relay-rpc.com 1001 --monitor-blocks --fix
```

## ⚠️ Safety Considerations

### Dangerous Flags

- `--force-cleanup` - Forces data cleanup (can cause network instability)
- `--yes` - Skips all confirmations (use only in automation)
- `--force-reregister` - Complete data wipe and re-registration

### Best Practices

1. **Always test on local networks first**
2. **Use `--verbose` for detailed diagnostics**
3. **Use `--monitor-blocks` to verify block production**
4. **Never use `--yes` flag interactively**
5. **Keep backups of important parachain state**
6. **Verify operations with diagnostic scripts**

## 🔧 CLI Reference

### Unified Parameters

Both scripts use consistent parameter naming:

| Parameter | Description | Used In |
|-----------|-------------|---------|
| `--seed "0x..."` | Custom sudo account seed (default: //Alice) | Both scripts |
| `--verbose` | Show detailed diagnostic information | diagnose-parachain.py |
| `--wait` | Wait for operation completion | onboard-parachain.py |
| `--fix` | Automatically attempt to fix issues | diagnose-parachain.py |
| `--yes` | Skip confirmation prompts | onboard-parachain.py |

### Script-Specific Parameters

**onboard-parachain.py:**
| Parameter | Description |
|-----------|-------------|
| `--force-reregister` | Deregister then register parachain |
| `--deregister-only` | Only deregister, don't register |
| `--para-id 1001` | Parachain ID (required for deregister-only) |
| `--lease-periods 100` | Number of lease periods |
| `--force-cleanup` | Force cleanup if normal deregistration fails |

**diagnose-parachain.py:**
| Parameter | Description |
|-----------|-------------|
| `--monitor-blocks` | Monitor parachain block production for 30 seconds |

## 📝 Version History

### v2.0 - Unified Diagnostic & Test Network Optimization
- ✅ Combined diagnostic and collation checking into single script
- ✅ Added parachain deregistration support
- ✅ `--force-reregister` flag for pre-deploy workflows  
- ✅ Enhanced safety confirmations with destructive operation warnings
- ✅ Real-time block production monitoring with `--monitor-blocks`
- ✅ Automatic issue fixing with `--fix` flag
- ✅ Simplified session handling optimized for test networks
- ✅ Removed dependency on unavailable runtime functions
- ✅ Unified CLI parameter naming across all scripts
- ✅ Comprehensive parachain health checking and remediation

## 🔗 Additional Resources

- [Polkadot Wiki - Parachains](https://wiki.polkadot.network/docs/learn-parachains)
- [Cumulus Tutorial](https://docs.substrate.io/tutorials/build-a-parachain/)
- [Substrate Interface Documentation](https://polkascan.github.io/py-substrate-interface/)

## 🔗 License

This project is licensed under the [Apache License 2.0](../LICENSE).