#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import sys
import time
import argparse
import logging
from datetime import datetime
from lib import (
    substrate_sudo_call,
    force_set_current_code
)

from substrateinterface import SubstrateInterface, Keypair

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_mark(condition):
    """Return checkmark or cross based on condition"""
    return "✓" if condition else "✗"

def check_parachain_head_updates(relay_chain_client, para_id, check_duration=30):
    """Check if parachain head is being updated (blocks are being produced)"""
    logger.info(f"Monitoring parachain head updates for {check_duration} seconds...")

    initial_head = None
    head_updates = []

    try:
        initial_head = relay_chain_client.query('Paras', 'Heads', params=[para_id])
        if not initial_head.value:
            logger.error("No parachain head found!")
            return False, "No head data"

        initial_head_hash = initial_head.value[:64]
        logger.info(f"Initial head: {initial_head_hash}...")

        start_time = time.time()
        last_check = start_time

        while time.time() - start_time < check_duration:
            current_time = time.time()
            if current_time - last_check >= 6:  # Check every 6 seconds
                current_head = relay_chain_client.query('Paras', 'Heads', params=[para_id])
                if current_head.value:
                    current_head_hash = current_head.value[:64]
                    if current_head_hash != initial_head_hash:
                        head_updates.append({
                            'time': current_time - start_time,
                            'head': current_head_hash
                        })
                        logger.info(f"  Head updated after {current_time - start_time:.1f}s: {current_head_hash}...")
                        initial_head_hash = current_head_hash
                last_check = current_time
            time.sleep(1)

        if head_updates:
            logger.info(f"✓ Parachain head updated {len(head_updates)} times in {check_duration} seconds")
            return True, f"{len(head_updates)} updates"
        else:
            logger.warning(f"✗ Parachain head did NOT update in {check_duration} seconds")
            return False, "No updates"

    except Exception as e:
        logger.error(f"Error checking head updates: {e}")
        return False, str(e)

def check_pending_availability(relay_chain_client, para_id):
    """Check if there are pending availability issues"""
    try:
        # Try to check PendingAvailability
        pending = relay_chain_client.query('ParaInclusion', 'PendingAvailability', params=[para_id])
        if pending.value:
            logger.warning(f"Found pending availability: {pending.value}")
            return True, pending.value
        else:
            logger.info("No pending availability issues")
            return False, None
    except:
        # Try alternative storage location
        try:
            pending = relay_chain_client.query('ParaInclusion', 'PendingAvailabilityCommitments', params=[para_id])
            if pending.value:
                logger.warning(f"Found pending availability commitments: {pending.value}")
                return True, pending.value
            else:
                logger.info("No pending availability commitments")
                return False, None
        except:
            logger.info("Could not check pending availability (may not be available in this runtime)")
            return False, None

def force_include_parachain(relay_chain_client, sudo_keypair, para_id):
    """Force include parachain in the next block"""
    logger.info(f"Attempting to force include parachain {para_id}...")

    try:
        # Force set current code if needed
        validation_code = relay_chain_client.query('Paras', 'CurrentCodeHash', params=[para_id])
        if not validation_code.value:
            logger.warning("No validation code found, trying to set it...")
            force_set_current_code(relay_chain_client, sudo_keypair, para_id)
            time.sleep(5)

        # Force queue action
        call = relay_chain_client.compose_call(
            call_module='Paras',
            call_function='force_queue_action',
            call_params={
                'para': para_id
            }
        )

        result = substrate_sudo_call(relay_chain_client, sudo_keypair, call, wait=True)
        if result:
            logger.info("✓ Queue action forced successfully")
            return True
        else:
            logger.error("✗ Failed to force queue action")
            return False

    except Exception as e:
        logger.error(f"Error forcing parachain inclusion: {e}")
        return False

def diagnose_parachain(relay_url, para_id, verbose=False, monitor_blocks=False, apply_fixes=False, sudo_seed=None):
    """Comprehensive parachain diagnostics with optional fixes"""

    try:
        relay_chain_client = SubstrateInterface(url=relay_url)
    except Exception as e:
        logger.error(f"Failed to connect to relay chain: {e}")
        return 1

    logger.info(f"Diagnosing parachain {para_id} on {relay_url}")
    logger.info("=" * 60)

    issues_found = []
    warnings = []

    # 1. Check if parachain is registered
    logger.info("\n1. Registration Status:")
    parachains = relay_chain_client.query('Paras', 'Parachains', params=[])
    is_registered = para_id in parachains.value
    logger.info(f"  {check_mark(is_registered)} Parachain {para_id} is {'registered' if is_registered else 'NOT registered'}")

    if not is_registered:
        logger.error(f"  Registered parachains: {parachains.value}")
        issues_found.append("Parachain is not registered")
        return 1

    # 2. Check parachain lifecycle
    logger.info("\n2. Lifecycle Status:")
    lifecycle = relay_chain_client.query('Paras', 'ParaLifecycles', params=[para_id])
    is_active = lifecycle.value == 'Parachain'
    logger.info(f"  {check_mark(is_active)} Lifecycle state: {lifecycle.value}")

    if lifecycle.value != 'Parachain':
        if lifecycle.value == 'Onboarding':
            warnings.append(f"Parachain is still onboarding")
        elif lifecycle.value == 'Parathread':
            warnings.append(f"Parachain is registered as parathread")
        else:
            issues_found.append(f"Parachain is in unexpected state: {lifecycle.value}")

    # 3. Check if parachain has a lease
    logger.info("\n3. Lease Status:")
    current_lease = relay_chain_client.query('Slots', 'Leases', params=[para_id])
    has_lease = bool(current_lease.value)
    logger.info(f"  {check_mark(has_lease)} Has lease: {'Yes' if has_lease else 'No'}")

    if has_lease and verbose:
        logger.info(f"  Lease details: {current_lease.value}")
    elif not has_lease:
        warnings.append("Parachain has no active lease")

    # 4. Check validator groups
    logger.info("\n4. Validator Assignment:")
    num_groups = 0
    try:
        validator_groups = relay_chain_client.query('ParaScheduler', 'ValidatorGroups', params=[])
        num_groups = len(validator_groups.value) if validator_groups.value else 0
    except:
        # Try alternative way to check validators
        try:
            validators = relay_chain_client.query('Session', 'Validators', params=[])
            if validators.value:
                num_groups = 1  # Assume at least one group if we have validators
                logger.info("  Note: ValidatorGroups not available, checking validators directly")
        except:
            pass

    logger.info(f"  Number of validator groups: {num_groups}")

    if num_groups == 0:
        issues_found.append("No validator groups found")

    # 5. Check scheduled cores
    logger.info("\n5. Core Assignment:")
    para_scheduled = False
    assigned_core = None

    # Try different storage locations for different runtime versions
    try:
        # Try new location first
        scheduled = relay_chain_client.query('ParaScheduler', 'Scheduled', params=[])
        if scheduled.value:
            for idx, core_assignment in enumerate(scheduled.value):
                # Check both old and new format
                if 'assignment' in core_assignment:
                    assignment = core_assignment['assignment']
                    if 'Para' in assignment and assignment['Para'] == para_id:
                        para_scheduled = True
                        assigned_core = idx
                        break
                elif 'Para' in core_assignment and core_assignment['Para'] == para_id:
                    para_scheduled = True
                    assigned_core = idx
                    break
    except:
        # Try older API
        try:
            # Check if parachain is in availability cores
            availability_cores = relay_chain_client.query('ParaScheduler', 'AvailabilityCores', params=[])
            if availability_cores.value:
                for idx, core in enumerate(availability_cores.value):
                    if core and 'Para' in str(core) and str(para_id) in str(core):
                        para_scheduled = True
                        assigned_core = idx
                        break
        except:
            # Fall back to checking if parachain is active
            logger.info("  Note: Core scheduling information not available in this runtime version")
            # If parachain is active and has validators, assume it's scheduled
            if lifecycle.value == 'Parachain' and num_groups > 0:
                para_scheduled = True
                logger.info("  Assuming scheduled based on active status")

    logger.info(f"  {check_mark(para_scheduled)} Scheduled on core: {'Yes (Core ' + str(assigned_core) + ')' if assigned_core is not None else ('Yes' if para_scheduled else 'No')}")

    if not para_scheduled:
        issues_found.append(f"Parachain {para_id} is NOT scheduled on any core")

    # 6. Check validators
    logger.info("\n6. Validators:")
    validators = relay_chain_client.query('Session', 'Validators', params=[])
    num_validators = len(validators.value) if validators.value else 0
    logger.info(f"  {check_mark(num_validators > 0)} Number of validators: {num_validators}")

    if num_validators == 0:
        issues_found.append("No validators found in the network")
    elif num_validators < 2:
        warnings.append(f"Only {num_validators} validator(s) found, minimum 2 recommended")

    # 7. Check session info
    logger.info("\n7. Session Info:")
    current_session = relay_chain_client.query('Session', 'CurrentIndex', params=[])
    logger.info(f"  Current session index: {current_session.value}")

    # 8. Check parachain head
    logger.info("\n8. Parachain Head:")
    para_head = relay_chain_client.query('Paras', 'Heads', params=[para_id])
    has_head = bool(para_head.value)
    logger.info(f"  {check_mark(has_head)} Has head data: {'Yes' if has_head else 'No'}")

    if has_head and verbose:
        logger.info(f"  Head: {para_head.value[:64]}...")
    elif not has_head:
        issues_found.append("No parachain head found")

    # 9. Check pending availability
    logger.info("\n9. Pending Availability:")
    has_pending, pending_data = check_pending_availability(relay_chain_client, para_id)
    if has_pending:
        warnings.append("Has pending availability - blocks may be stuck")

    # 10. Check validation code
    logger.info("\n10. Validation Code:")
    validation_code_hash = relay_chain_client.query('Paras', 'CurrentCodeHash', params=[para_id])
    has_code = bool(validation_code_hash.value)
    logger.info(f"  {check_mark(has_code)} Has validation code: {'Yes' if has_code else 'No'}")

    if not has_code:
        issues_found.append("No validation code found")

    # 11. Check future code upgrades
    logger.info("\n11. Code Upgrades:")
    has_future_code = False
    try:
        future_code = relay_chain_client.query('Paras', 'FutureCodeHash', params=[para_id])
        has_future_code = bool(future_code.value)
        logger.info(f"  Pending code upgrade: {'Yes' if has_future_code else 'No'}")

        if has_future_code:
            try:
                upgrade_at = relay_chain_client.query('Paras', 'FutureCodeUpgrades', params=[para_id])
                if upgrade_at.value:
                    logger.info(f"  Upgrade scheduled at block: {upgrade_at.value}")
            except:
                pass
    except:
        logger.info("  Note: Code upgrade check not available in this runtime version")

    # 12. Check block production (if requested)
    is_producing = True
    production_status = "Not checked"

    if monitor_blocks:
        logger.info("\n12. Block Production Monitoring:")
        is_producing, production_status = check_parachain_head_updates(relay_chain_client, para_id, check_duration=30)
        if not is_producing:
            issues_found.append(f"Parachain head not updating: {production_status}")

    # 13. Check current block heights
    logger.info(f"\n{13 if monitor_blocks else 12}. Block Heights:")
    try:
        relay_block = relay_chain_client.get_block()
        relay_height = relay_block['header']['number']
        logger.info(f"  Relay chain block height: {relay_height}")

        para_head = relay_chain_client.query('Paras', 'Heads', params=[para_id])
        if para_head.value:
            logger.info(f"  Parachain has head data")
    except Exception as e:
        logger.error(f"  Error checking blocks: {e}")

    # Summary and fixes
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS SUMMARY")
    logger.info("=" * 60)

    if not issues_found and not warnings:
        logger.info("✅ No critical issues found!")
        if monitor_blocks and not is_producing:
            logger.info("\nHowever, blocks are not being produced. Possible reasons:")
            logger.info("  1. Collators can't reach validators (network issue)")
            logger.info("  2. Collators have wrong relay chain spec")
            logger.info("  3. Collators are not running or crashed")
    else:
        if issues_found:
            logger.error("\n❌ CRITICAL ISSUES:")
            for issue in issues_found:
                logger.error(f"  - {issue}")

        if warnings:
            logger.warning("\n⚠️  WARNINGS:")
            for warning in warnings:
                logger.warning(f"  - {warning}")

    # Apply fixes if requested
    if apply_fixes and (issues_found or warnings or (monitor_blocks and not is_producing)):
        logger.info("\n" + "=" * 60)
        logger.info("APPLYING FIXES")
        logger.info("=" * 60)

        if sudo_seed:
            sudo_keypair = Keypair.create_from_seed(sudo_seed, relay_chain_client.ss58_format)
        else:
            sudo_keypair = Keypair.create_from_uri('//Alice', relay_chain_client.ss58_format)

        logger.info(f"Using sudo account: {sudo_keypair.ss58_address}")

        # Try to force include the parachain
        if force_include_parachain(relay_chain_client, sudo_keypair, para_id):
            logger.info("\n✓ Applied force include fix")

            if monitor_blocks:
                logger.info("Waiting 30 seconds to check if it helped...")
                time.sleep(30)

                # Re-check block production
                logger.info("\nRe-checking block production:")
                is_producing_after, status_after = check_parachain_head_updates(relay_chain_client, para_id, check_duration=20)

                if is_producing_after:
                    logger.info("✅ Fix successful! Blocks are now being produced!")
                else:
                    logger.warning("⚠️  Blocks still not being produced after fix")
                    logger.info("\nNext steps to try:")
                    logger.info("  1. Check collator logs for errors")
                    logger.info("  2. Restart collators if needed")
                    logger.info("  3. Check network connectivity between nodes")
                    logger.info("  4. Verify relay chain spec on collators matches validators")
        else:
            logger.error("✗ Failed to apply fixes")

    # Provide debugging recommendations
    if issues_found or warnings or (monitor_blocks and not is_producing):
        logger.info("\n" + "=" * 60)
        logger.info("RECOMMENDED ACTIONS")
        logger.info("=" * 60)

        if "not registered" in str(issues_found).lower():
            logger.info("  1. Run onboard-parachain.py to register the parachain")
        elif "not scheduled" in str(issues_found).lower() or "no validator groups" in str(issues_found).lower():
            logger.info("  1. Wait for next session rotation (1-2 minutes)")
            logger.info("  2. Check if validators are properly configured")
            logger.info("  3. Run this script with --fix to attempt automatic fixes")
        elif "onboarding" in str(warnings).lower():
            logger.info("  1. Wait for onboarding to complete")
            logger.info("  2. Use --fix flag to force queue actions")
        elif monitor_blocks and not is_producing:
            logger.info("  1. Check collator logs for network connectivity issues")
            logger.info("  2. Verify collators have correct relay chain spec")
            logger.info("  3. Restart collator nodes if needed")
            logger.info("  4. Run this script with --fix to attempt fixes")

    return 0 if not issues_found else 1

def main():
    parser = argparse.ArgumentParser(description='Diagnose parachain issues and optionally fix them')
    parser.add_argument('relay_url', help='WebSocket URL of the relay chain')
    parser.add_argument('para_id', type=int, help='Parachain ID to diagnose')
    parser.add_argument('--verbose', action='store_true',
                       help='Show verbose output with additional details')
    parser.add_argument('--monitor-blocks', action='store_true',
                       help='Monitor parachain block production for 30 seconds')
    parser.add_argument('--fix', action='store_true',
                       help='Attempt to fix issues automatically')
    parser.add_argument('--seed', help='Sudo account seed (default: //Alice)', default=None)

    args = parser.parse_args()

    return diagnose_parachain(
        args.relay_url,
        args.para_id,
        args.verbose,
        args.monitor_blocks,
        args.fix,
        args.seed
    )

if __name__ == "__main__":
    sys.exit(main())
