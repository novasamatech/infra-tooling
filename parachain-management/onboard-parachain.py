#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import sys
import argparse
import logging
import time
from lib import (
    initialize_parachain,
    get_chain_wasm,
    get_parachain_head,
    deregister_parachain,
    wait_for_parachain_deregistration,
    force_parachain_cleanup,
    wait_for_parachain_activation,
    check_validator_groups
)

from substrateinterface import SubstrateInterface, Keypair

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def confirm_action(message, auto_yes=False):
    """Ask for user confirmation unless auto_yes is True"""
    if auto_yes:
        logger.info(f"{message} (auto-confirmed)")
        return True

    while True:
        response = input(f"{message} (y/N): ").lower().strip()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no', '']:
            return False
        else:
            print("Please answer 'y' or 'n'")

def get_parachain_status(relay_chain_client, para_id):
    """Get current parachain registration status"""
    try:
        parachains = relay_chain_client.query('Paras', 'Parachains', params=[])
        is_registered = para_id in parachains.value

        if is_registered:
            lifecycle = relay_chain_client.query('Paras', 'ParaLifecycles', params=[para_id])
            return {
                'registered': True,
                'lifecycle': lifecycle.value,
                'all_parachains': parachains.value
            }
        else:
            return {
                'registered': False,
                'lifecycle': None,
                'all_parachains': parachains.value
            }
    except Exception as e:
        logger.error(f"Error checking parachain status: {e}")
        return {'registered': False, 'lifecycle': None, 'error': str(e)}

def deregister_parachain_flow(relay_chain_client, sudo_keypair, para_id, force_cleanup_flag=False, auto_yes=False, wait=False):
    """Complete parachain deregistration flow"""
    logger.info(f"Starting deregistration of parachain {para_id}")

    status = get_parachain_status(relay_chain_client, para_id)
    if not status['registered']:
        logger.info(f"Parachain {para_id} is not registered")
        return True

    logger.info(f"Current parachain status: {status['lifecycle']}")

    if not auto_yes:
        print("\n" + "="*60)
        print("🚨 WARNING: PARACHAIN DEREGISTRATION")
        print("="*60)
        print(f"You are about to DEREGISTER parachain {para_id}")
        print(f"Current status: {status['lifecycle']}")
        print(f"All registered parachains: {status['all_parachains']}")
        print("\nThis will:")
        print("  - Remove the parachain from the relay chain")
        print("  - Clear all parachain data and state")
        print("  - Make the parachain ID available for reuse")
        print("\nThis action is IRREVERSIBLE!")
        print("="*60)

    if not confirm_action("Are you sure you want to deregister this parachain?", auto_yes):
        logger.info("Deregistration cancelled by user")
        return False

    # Attempt normal deregistration
    logger.info("Attempting graceful deregistration...")
    if deregister_parachain(relay_chain_client, sudo_keypair, para_id, cleanup_data=True):
        logger.info("Deregistration call submitted")

        if wait:
            logger.info("Waiting for deregistration to complete...")
            # Wait for deregistration to complete
            if wait_for_parachain_deregistration(relay_chain_client, para_id, max_attempts=20, delay=10):
                logger.info("✅ Parachain successfully deregistered!")
                return True
            else:
                logger.warning("Deregistration taking longer than expected...")

                if force_cleanup_flag:
                    if confirm_action("Try force cleanup? (DANGEROUS)", auto_yes):
                        logger.warning("Attempting force cleanup...")
                        if force_parachain_cleanup(relay_chain_client, sudo_keypair, para_id):
                            time.sleep(30)  # Wait for cleanup to process
                            final_status = get_parachain_status(relay_chain_client, para_id)
                            if not final_status['registered']:
                                logger.info("✅ Force cleanup successful!")
                                return True
                            else:
                                logger.error("❌ Force cleanup failed")
                                return False

                logger.warning("⚠️ Deregistration may still be in progress")
                return False
        else:
            logger.info("Deregistration initiated (use --wait to wait for completion)")
            return True
    else:
        logger.error("❌ Failed to initiate deregistration")
        return False

def register_parachain_flow(relay_chain_client, para_chain_client, sudo_keypair, args):
    """Complete parachain registration flow"""
    para_id = para_chain_client.query('ParachainInfo', 'ParachainId', params=[]).value
    logger.info(f"Starting registration of parachain {para_id}")

    # Get parachain data
    logger.info("Collecting parachain data...")
    state = get_parachain_head(para_chain_client)
    wasm = get_chain_wasm(para_chain_client)

    logger.info(f"Parachain ID: {para_id}")
    logger.info(f"Genesis state: {state[:32]}...{state[-32:]}")
    logger.info(f"Validation code: {wasm[:32]}...{wasm[-32:]}")

    if not args.yes:
        print("\n" + "="*60)
        print("📋 PARACHAIN REGISTRATION SUMMARY")
        print("="*60)
        print(f"Parachain ID: {para_id}")
        print(f"Relay chain: {args.relay_url}")
        print(f"Parachain: {args.para_url}")
        print(f"Lease periods: {args.lease_periods}")
        print("="*60)

    if not confirm_action("Proceed with parachain registration?", args.yes):
        logger.info("Registration cancelled by user")
        return False

    # Register parachain
    logger.info("Initiating parachain registration...")
    result = initialize_parachain(
        relay_chain_client,
        sudo_keypair,
        para_id,
        state,
        wasm,
        lease_period_count=args.lease_periods,
        force_queue_action=True,
        activate_parachain=False  # We'll handle activation manually
    )

    if not result:
        logger.error("❌ Failed to initialize parachain!")
        return False

    logger.info(f"✅ Registration transaction submitted: {result.extrinsic_hash}")

    # Wait for activation if requested
    if args.wait:
        logger.info("Waiting for parachain activation...")
        if wait_for_parachain_activation(relay_chain_client, para_id, max_attempts=30, delay=10):
            logger.info("✅ Parachain activated successfully!")

            # Check validator assignment
            if check_validator_groups(relay_chain_client, para_id):
                logger.info("✅ Validators assigned successfully!")
            else:
                logger.warning("⚠️ Validator assignment may need more time")
        else:
            logger.warning("⚠️ Parachain activation took longer than expected")
    else:
        logger.info("Registration completed (use --wait to wait for activation)")

    return True

def main():
    parser = argparse.ArgumentParser(
        description='Onboard parachains to relay chain (designed for test networks)'
    )

    parser.add_argument('relay_url', help='WebSocket URL of the relay chain')
    parser.add_argument('para_url', nargs='?', help='WebSocket URL of the parachain (not needed for deregister-only)')
    parser.add_argument('--seed', help='Sudo account seed (default: //Alice)', default=None)

    # Action options
    parser.add_argument('--deregister-only', action='store_true',
                       help='Only deregister the parachain, do not register')
    parser.add_argument('--force-reregister', action='store_true',
                       help='Deregister and then register the parachain')
    parser.add_argument('--para-id', type=int, help='Parachain ID (required for deregister-only)')

    # Registration options
    parser.add_argument('--lease-periods', type=int, default=100,
                       help='Number of lease periods (default: 100)')
    parser.add_argument('--wait', action='store_true',
                       help='Wait for activation/deregistration completion')

    # Safety options
    parser.add_argument('--force-cleanup', action='store_true',
                       help='Use force cleanup if normal deregistration fails (DANGEROUS)')
    parser.add_argument('--yes', action='store_true',
                       help='Skip all confirmation prompts (use with caution)')

    args = parser.parse_args()

    # Validate arguments
    if args.deregister_only and not args.para_id:
        parser.error("--para-id is required when using --deregister-only")

    if not args.deregister_only and not args.para_url:
        parser.error("para_url is required unless using --deregister-only")

    logger.info("🚀 Parachain Onboarding Tool for Test Networks")
    logger.info("=" * 60)

    # Connect to relay chain
    try:
        logger.info(f"Connecting to relay chain: {args.relay_url}")
        relay_chain_client = SubstrateInterface(url=args.relay_url)
        logger.info("✅ Connected to relay chain")
    except Exception as e:
        logger.error(f"❌ Failed to connect to relay chain: {e}")
        return 1

    # Connect to parachain (if needed)
    para_chain_client = None
    if not args.deregister_only:
        try:
            logger.info(f"Connecting to parachain: {args.para_url}")
            para_chain_client = SubstrateInterface(url=args.para_url)
            logger.info("✅ Connected to parachain")
        except Exception as e:
            logger.error(f"❌ Failed to connect to parachain: {e}")
            return 1

    # Setup sudo keypair
    if args.seed:
        logger.info("Using provided sudo seed")
        sudo_keypair = Keypair.create_from_seed(args.seed, relay_chain_client.ss58_format)
    else:
        logger.info("Using default //Alice account")
        sudo_keypair = Keypair.create_from_uri('//Alice', relay_chain_client.ss58_format)

    logger.info(f"Sudo account: {sudo_keypair.ss58_address}")

    # Determine parachain ID
    if args.para_id:
        para_id = args.para_id
    elif para_chain_client:
        para_id = para_chain_client.query('ParachainInfo', 'ParachainId', params=[]).value
    else:
        logger.error("Could not determine parachain ID")
        return 1

    logger.info(f"Working with parachain ID: {para_id}")

    # Check current status
    status = get_parachain_status(relay_chain_client, para_id)
    logger.info(f"Current registration status: {'Registered' if status['registered'] else 'Not registered'}")
    if status['registered']:
        logger.info(f"Current lifecycle: {status['lifecycle']}")

    success = True

    # Execute requested actions
    if args.deregister_only or args.force_reregister:
        # Deregister parachain
        if status['registered']:
            success = deregister_parachain_flow(
                relay_chain_client,
                sudo_keypair,
                para_id,
                force_cleanup_flag=args.force_cleanup,
                auto_yes=args.yes,
                wait=args.wait
            )
            if not success:
                logger.error("❌ Deregistration failed!")
                return 1
        else:
            logger.info("Parachain is not registered, skipping deregistration")

    if not args.deregister_only and success:
        # Register parachain
        if args.force_reregister or not status['registered']:
            success = register_parachain_flow(
                relay_chain_client,
                para_chain_client,
                sudo_keypair,
                args
            )
            if not success:
                logger.error("❌ Registration failed!")
                return 1
        else:
            logger.warning(f"Parachain {para_id} is already registered!")
            logger.info("Use --force-reregister to deregister and register again")
            return 0

    if success:
        logger.info("🎉 All operations completed successfully!")

        if not args.deregister_only:
            logger.info("\n📋 Next steps:")
            logger.info("  1. Check parachain logs for any errors")
            logger.info("  2. Monitor block production and finalization")
            logger.info("  3. Use diagnose-parachain.py for detailed status")
            logger.info("  4. Use check-collation.py if collators have issues")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
