# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

from math import floor
import logging as log
import time

def substrate_call(substrate_client, keypair, call, wait=True):
    if keypair:
        extrinsic = substrate_client.create_signed_extrinsic(
            call=call,
            keypair=keypair,
        )
    else:
        extrinsic = substrate_client.create_unsigned_extrinsic(
            call=call
        )

    try:
        receipt = substrate_client.submit_extrinsic(extrinsic, wait_for_inclusion=wait)
        log.info("Extrinsic '{}' sent".format(receipt.extrinsic_hash))
        return receipt
    except Exception as e:
        log.error("Failed to send call: {}, Error: {}".format(call, e))
        return False

def substrate_check_sudo_key_and_call(substrate_client, keypair, payload, wait=True):
    sudo_keys = substrate_client.query('Sudo', 'Key', params=[]).value
    provided_key = keypair.ss58_address
    if provided_key == sudo_keys:
        return substrate_call(substrate_client, keypair, payload, wait)
    else:
        log.error(f"Failed to execute sudo call: {getattr(substrate_client, 'url', 'NO_URL')} {payload.value['call_module']}.{payload.value['call_function']}, Error: Provided wrong sudo key {provided_key}, expected {sudo_keys}")
        return None

def substrate_sudo_call(substrate_client, keypair, payload, wait=True):
    call = substrate_client.compose_call(
        call_module='Sudo',
        call_function='sudo',
        call_params={
            'call': payload.value,
        }
    )
    return substrate_check_sudo_key_and_call(substrate_client, keypair, call, wait)

def substrate_batchall_call(substrate_client, keypair, batch_call, wait=True):
    # If the batch contains only 1 element, don't use batch
    if len(batch_call) == 1:
        call = batch_call[0]
    else:
        call = substrate_client.compose_call(
            call_module='Utility',
            call_function='batch',
            call_params={
                'calls': batch_call
            }
        )

    return substrate_sudo_call(substrate_client, keypair, call, wait)

def get_lease_period_duration(substrate_client):
    return substrate_client.get_constant("Slots", "LeasePeriod").value

def initialize_parachain(substrate_client, sudo_keypair, para_id, state, wasm, lease_period_count=100, force_queue_action=True, activate_parachain=True):
    batch_call = []
    batch_call.append(substrate_client.compose_call(
        call_module='ParasSudoWrapper',
        call_function='sudo_schedule_para_initialize',
        call_params={
            'id': para_id,
            'genesis': {
                'genesis_head': state,
                'validation_code': wasm,
                'parachain': True, # legacy param
                'para_kind': True # new param introduced in https://github.com/paritytech/polkadot/pull/6198
            },
            'initial_delay': 0
        }
    ))
    if lease_period_count != 0:
        lease_period_duration = get_lease_period_duration(substrate_client)
        block_height = substrate_client.get_block()['header']['number']
        current_lease_period_number = floor(block_height / lease_period_duration)
        batch_call.append(substrate_client.compose_call(
            call_module='Slots',
            call_function='force_lease',
            call_params={
                'para': para_id,
                'leaser': sudo_keypair.ss58_address,
                'amount': 0,
                'period_begin': current_lease_period_number,
                'period_count': lease_period_count
            }
        ))
    if force_queue_action:
        batch_call.append(substrate_client.compose_call(
            call_module='Paras',
            call_function='force_queue_action',
            call_params={
                'para': para_id
            }
        ))

    result = substrate_batchall_call(substrate_client, sudo_keypair, batch_call, True)

    if result and activate_parachain:
        # Wait for parachain to be registered
        time.sleep(10)

        # Wait for activation
        if wait_for_parachain_activation(substrate_client, para_id):
            log.info("Parachain activated successfully")

            # Check if validators are assigned
            if not check_validator_groups(substrate_client, para_id):
                log.warning("Validators may not be properly assigned to the parachain")
                # Try to force queue action again
                force_set_current_code(substrate_client, sudo_keypair, para_id)
        else:
            log.error("Parachain did not activate in time")

    return result

def get_chain_wasm(node_client):
    # query for Substrate.Code see: https://github.com/polkascan/py-substrate-interface/issues/190
    block_hash = node_client.get_chain_head()
    parachain_wasm = node_client.get_storage_by_key(block_hash, "0x3a636f6465")
    return parachain_wasm

def convert_header(plain_header, substrate):
    raw_header = '0x'
    raw_header += plain_header['parentHash'].replace('0x', '')
    raw_header += str(substrate.encode_scale('Compact<u32>', int(plain_header['number'], 16))).replace('0x', '')
    raw_header += plain_header['stateRoot'].replace('0x', '')
    raw_header += plain_header['extrinsicsRoot'].replace('0x', '')
    raw_header += str(substrate.encode_scale('Compact<u32>', len(plain_header['digest']['logs']))).replace('0x', '')
    for lg in plain_header['digest']['logs']:
        raw_header += lg.replace('0x', '')
    return raw_header

def get_parachain_head(node_client):
    block_header = node_client.rpc_request(method="chain_getHeader", params=[])
    return convert_header(block_header['result'], node_client)

def get_permanent_slot_lease_period_length(substrate_client):
    return substrate_client.get_constant("AssignedSlots", "PermanentSlotLeasePeriodLength").value

def wait_for_parachain_activation(substrate_client, para_id, max_attempts=30, delay=10):
    """Wait for parachain to become active"""
    log.info(f"Waiting for parachain {para_id} to become active...")

    for attempt in range(max_attempts):
        try:
            # Check if parachain is registered
            parachain_info = substrate_client.query('Paras', 'Parachains', params=[])
            if para_id in parachain_info.value:
                # Check parachain lifecycle state
                lifecycle = substrate_client.query('Paras', 'ParaLifecycles', params=[para_id])
                log.info(f"Parachain {para_id} lifecycle: {lifecycle.value}")

                if lifecycle.value == 'Parachain':
                    log.info(f"Parachain {para_id} is now active!")
                    return True
        except Exception as e:
            log.warning(f"Error checking parachain status: {e}")

        log.info(f"Attempt {attempt + 1}/{max_attempts}: Parachain not active yet...")
        time.sleep(delay)

    return False



def check_validator_groups(substrate_client, para_id):
    """Check if validators are assigned to the parachain"""
    log.info(f"Checking validator groups for parachain {para_id}...")

    # Get current session index
    try:
        session_index = substrate_client.query('Session', 'CurrentIndex', params=[]).value
        log.info(f"Current session index: {session_index}")
    except Exception as e:
        log.warning(f"Could not get session index: {e}")

    # Check validator groups
    try:
        validator_groups = substrate_client.query('ParaScheduler', 'ValidatorGroups', params=[])
        log.info(f"Validator groups: {validator_groups.value}")
    except Exception as e:
        log.warning(f"ValidatorGroups not available: {e}")
        # Try to check if we have validators at all
        try:
            validators = substrate_client.query('Session', 'Validators', params=[])
            if validators.value:
                log.info(f"Found {len(validators.value)} validators in session")
        except:
            pass

    # Check scheduled parachains - try different storage locations
    try:
        scheduled = substrate_client.query('ParaScheduler', 'Scheduled', params=[])
        log.info(f"Scheduled parachains: {scheduled.value}")

        # Check if our parachain is scheduled
        if scheduled.value:
            for core_assignment in scheduled.value:
                if 'assignment' in core_assignment:
                    assignment = core_assignment['assignment']
                    if 'Para' in assignment and assignment['Para'] == para_id:
                        log.info(f"Parachain {para_id} is scheduled on a core")
                        return True
                # Also check for older format
                if 'Para' in core_assignment and core_assignment['Para'] == para_id:
                    log.info(f"Parachain {para_id} is scheduled on a core")
                    return True
    except Exception as e:
        log.warning(f"Scheduled storage not available: {e}")

        # Try alternative: check AvailabilityCores
        try:
            availability_cores = substrate_client.query('ParaScheduler', 'AvailabilityCores', params=[])
            if availability_cores.value:
                for idx, core in enumerate(availability_cores.value):
                    if core and 'Para' in str(core) and str(para_id) in str(core):
                        log.info(f"Parachain {para_id} found in availability core {idx}")
                        return True
        except Exception as e2:
            log.warning(f"AvailabilityCores not available: {e2}")

            # Final fallback: if parachain is active, assume it has validators
            try:
                lifecycle = substrate_client.query('Paras', 'ParaLifecycles', params=[para_id])
                if lifecycle.value == 'Parachain':
                    log.info(f"Parachain {para_id} is active, assuming validators are assigned")
                    return True
            except:
                pass

    return False

def force_set_current_code(substrate_client, sudo_keypair, para_id):
    """Force set current validation code for parachain"""
    log.info(f"Forcing validation code update for parachain {para_id}...")

    # Get the current validation code
    validation_code = substrate_client.query('Paras', 'CurrentCodeHash', params=[para_id])
    if not validation_code.value:
        log.error(f"No validation code found for parachain {para_id}")
        return False

    # Force queue action to process any pending upgrades
    call = substrate_client.compose_call(
        call_module='Paras',
        call_function='force_queue_action',
        call_params={
            'para': para_id
        }
    )

    result = substrate_sudo_call(substrate_client, sudo_keypair, call, wait=True)
    return result

def deregister_parachain(substrate_client, sudo_keypair, para_id, cleanup_data=True):
    """Deregister a parachain (for test networks only!)"""
    log.info(f"Deregistering parachain {para_id}...")

    batch_call = []

    # First, try to deregister the parachain
    try:
        batch_call.append(substrate_client.compose_call(
            call_module='ParasSudoWrapper',
            call_function='sudo_schedule_para_cleanup',
            call_params={
                'id': para_id
            }
        ))
    except:
        # Alternative method for older runtimes
        try:
            batch_call.append(substrate_client.compose_call(
                call_module='Paras',
                call_function='force_schedule_code_upgrade',
                call_params={
                    'para': para_id,
                    'new_code': b'',  # Empty code to trigger cleanup
                    'relay_parent_number': 0
                }
            ))
        except Exception as e:
            log.error(f"Could not create deregistration call: {e}")
            return False

    # Clean up lease data if requested
    if cleanup_data:
        try:
            batch_call.append(substrate_client.compose_call(
                call_module='Slots',
                call_function='clear_all_leases',
                call_params={
                    'para': para_id
                }
            ))
        except:
            # Try alternative cleanup
            try:
                batch_call.append(substrate_client.compose_call(
                    call_module='Slots',
                    call_function='force_lease',
                    call_params={
                        'para': para_id,
                        'leaser': sudo_keypair.ss58_address,
                        'amount': 0,
                        'period_begin': 0,
                        'period_count': 0
                    }
                ))
            except:
                log.warning("Could not clear lease data")

    # Force queue action to process cleanup
    batch_call.append(substrate_client.compose_call(
        call_module='Paras',
        call_function='force_queue_action',
        call_params={
            'para': para_id
        }
    ))

    if batch_call:
        result = substrate_batchall_call(substrate_client, sudo_keypair, batch_call, True)
        if result:
            log.info(f"Parachain {para_id} deregistration initiated")
            return result
        else:
            log.error("Failed to deregister parachain")
            return False

    return False

def wait_for_parachain_deregistration(substrate_client, para_id, max_attempts=30, delay=5):
    """Wait for parachain to be deregistered"""
    log.info(f"Waiting for parachain {para_id} to be deregistered...")

    for attempt in range(max_attempts):
        try:
            # Check if parachain is still registered
            parachain_info = substrate_client.query('Paras', 'Parachains', params=[])
            if para_id not in parachain_info.value:
                log.info(f"Parachain {para_id} successfully deregistered!")
                return True

            # Check lifecycle state
            lifecycle = substrate_client.query('Paras', 'ParaLifecycles', params=[para_id])
            if lifecycle.value and lifecycle.value not in ['Parachain', 'Parathread']:
                log.info(f"Parachain {para_id} lifecycle changed to: {lifecycle.value}")
                if lifecycle.value in ['DownwardQueueOpen', 'OffboardingParachain']:
                    log.info("Parachain is in offboarding state")
        except Exception as e:
            log.warning(f"Error checking deregistration status: {e}")

        log.info(f"Attempt {attempt + 1}/{max_attempts}: Parachain still registered...")
        time.sleep(delay)

    log.warning("Parachain may still be in process of deregistration")
    return False

def force_parachain_cleanup(substrate_client, sudo_keypair, para_id):
    """Force cleanup of parachain data (nuclear option for test networks)"""
    log.warning(f"Force cleaning parachain {para_id} data - THIS IS FOR TEST NETWORKS ONLY!")

    cleanup_calls = []

    # Try to force set empty code
    try:
        cleanup_calls.append(substrate_client.compose_call(
            call_module='Paras',
            call_function='force_set_current_code',
            call_params={
                'para': para_id,
                'new_code': b''
            }
        ))
    except:
        pass

    # Try to force set empty head
    try:
        cleanup_calls.append(substrate_client.compose_call(
            call_module='Paras',
            call_function='force_set_current_head',
            call_params={
                'para': para_id,
                'new_head': b''
            }
        ))
    except:
        pass

    # Force queue action multiple times
    for _ in range(3):
        cleanup_calls.append(substrate_client.compose_call(
            call_module='Paras',
            call_function='force_queue_action',
            call_params={
                'para': para_id
            }
        ))

    if cleanup_calls:
        result = substrate_batchall_call(substrate_client, sudo_keypair, cleanup_calls, True)
        if result:
            log.info("Force cleanup initiated")
            return result

    return False
