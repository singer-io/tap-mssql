"""
Patch in_memory backend so run_sync_mode does not raise on tap failure.
Instead it stores exit status with tap_error_message/discovery_error_message
so tests can inspect them, matching the stitch backend behavior.
"""
import os
import json
import time
from subprocess import Popen, PIPE

from tap_tester.backends.in_memory import InMemoryBackend
from tap_tester.logger import LOGGER


def _run_sync_mode(self, scenario, conn_id):
    conn = self.connections.get(conn_id, {})
    config = {**conn.get('properties'), **conn.get('credentials')}
    if config is None:
        raise Exception("Config not found for connection {}. Must ensure_connection before running the tap.".format(conn_id))

    config_path = '/tmp/tap_tester_config.json'
    with open(config_path, 'w') as f:
        json.dump(config, f)

    # Discovery
    LOGGER.info('**************************************************************************************************************')
    discover_command = [os.getenv("STITCH_TAP_PATH", ""),
                        "--config",
                        config_path,
                        "--discover"]
    LOGGER.info(f"Running discovery mode: {' '.join(discover_command)}")

    discover = Popen(discover_command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    disc_err = []
    for outline, errline in InMemoryBackend._InMemoryBackend__read_pipes([discover], discover.stdout, discover.stderr):
        if errline:
            disc_err.append(errline.rstrip())
            LOGGER.info(f"\t{errline.rstrip()}")

    catalog = self.catalogs.get(conn_id)
    if catalog is None:
        raise Exception("Catalog not found for connection {}. Must run check mode before running sync mode.".format(conn_id))

    catalog_path = '/tmp/tap_tester_catalog.json'
    with open(catalog_path, 'w') as f:
        json.dump(catalog, f)

    state_path = '/tmp/tap_tester_state.json'
    with open(state_path, 'w') as f:
        json.dump(self.get_state(conn_id), f)

    # Sync
    LOGGER.info('**************************************************************************************************************')
    sync_command = [os.getenv("STITCH_TAP_PATH", ""),
                    "--config",
                    config_path,
                    "--catalog",
                    catalog_path,
                    "--properties",
                    catalog_path,
                    "--state",
                    state_path]

    target_command = [os.getenv("STITCH_TARGET_PATH", ""),
                        "--output-file",
                        self.target_output_file,
                        "--dry-run"]
    LOGGER.info(f"Running sync mode: {' '.join(sync_command)} | {' '.join(target_command)}")

    sync = Popen(sync_command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    target = Popen(target_command, stdin=sync.stdout, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    raw_state = ""
    tap_err = []
    for syncerr, targetout, targeterr in InMemoryBackend._InMemoryBackend__read_pipes([sync, target], sync.stderr, target.stdout, target.stderr):
        if targetout:
            raw_state = targetout
            LOGGER.info(f"\t main - new state: {raw_state.rstrip()}")
        if syncerr:
            tap_err.append(syncerr.rstrip())
            LOGGER.info(f"\t tap - {syncerr.rstrip()}")
        if targeterr:
            LOGGER.info(f"\t target - {targeterr.rstrip()}")

    state = json.loads(raw_state) if raw_state else {}
    self.set_state(conn_id, state)

    job_name = '{}-{}'.format(conn_id, str(int(time.time())))
    job = {"discovery_exit_status": discover.returncode,
           "check_exit_status": None,
           "tap_exit_status": sync.returncode,
           "target_exit_status": target.returncode}

    if discover.returncode not in (0, None):
        job["discovery_error_message"] = "\n".join(disc_err)
    if sync.returncode not in (0, None) or target.returncode not in (0, None) or discover.returncode not in (0, None):
        job["tap_error_message"] = "\n".join(tap_err)
        LOGGER.info('sync mode failed with tap exit status %s', sync.returncode)
    else:
        LOGGER.info('sync mode exitted succesfully')
    LOGGER.info('**************************************************************************************************************')
    self.jobs[job_name] = job
    return job_name


InMemoryBackend.run_sync_mode = _run_sync_mode
