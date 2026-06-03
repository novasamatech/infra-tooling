# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Test runner for the CoTURN connectivity tool (app.py).
#
# Server, ports and credentials are taken from ENVIRONMENT VARIABLES and have
# NO defaults — export them before running:
#
#   export TURN_HOST=turn.example.com   # hostname (required for the TLS WebRTC cert check)
#   export TURN_PORT=3478               # UDP/TCP STUN/TURN port
#   export TURN_TLS_PORT=5349           # TLS (turns:) port
#   export TURN_USER=myuser
#   export TURN_PASS=mypass
#   make test
#
# The virtualenv is expected next to this Makefile (./venv). If it is absent it
# is created automatically and ./requirements.txt is installed into it.

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT          := $(dir $(MAKEFILE_PATH))
VENV          := $(ROOT)venv
PY            := $(VENV)/bin/python
APP           := $(ROOT)app.py
REQ           := $(ROOT)requirements.txt
RUN           := "$(PY)" "$(APP)"

# Interpreter used to create the venv (override if your python3 lives elsewhere).
PYTHON ?= python3

# Tunables (NOT server/credentials, so defaults are fine).
TIMEOUT     ?= 30
MATRIX_RATE ?= 2
MATRIX_DUR  ?= 4
RATE_LOW    ?= 1
DUR_LOW     ?= 10
RATE_MOD    ?= 5
DUR_MOD     ?= 10
RATE_HIGH   ?= 50
DUR_HIGH    ?= 15

# Credentials, expanded into every invocation.
CREDS := -u "$(TURN_USER)" -p "$(TURN_PASS)"

# Export so values set on the command line (make TURN_HOST=...) also reach the
# recipe environment, matching values that were exported in the shell.
export TURN_HOST TURN_PORT TURN_TLS_PORT TURN_USER TURN_PASS

.DEFAULT_GOAL := help

.PHONY: help test all venv clean check-env \
        test-udp test-tcp test-tls test-guards test-speed

help:
	@echo "turn-testing — make targets:"
	@echo "  make venv         create ./venv and install requirements.txt (runs automatically)"
	@echo "  make test         full protocol: udp + tcp + tls + guards + speed"
	@echo "  make test-udp     UDP matrix    (stun / turn / webrtc / all-three)"
	@echo "  make test-tcp     TCP matrix"
	@echo "  make test-tls     TLS matrix    (+ --insecure smoke)"
	@echo "  make test-guards  argument-validation checks (each must exit 2)"
	@echo "  make test-speed   low + moderate (must pass) + high (informational)"
	@echo "  make clean        remove ./venv"
	@echo ""
	@echo "Required environment variables (no defaults):"
	@echo "  TURN_HOST TURN_PORT TURN_TLS_PORT TURN_USER TURN_PASS"

# ---------------------------------------------------------------- venv -----
# File target: only (re)build the venv when venv/bin/python is missing.
$(PY):
	@echo ">> creating virtualenv at $(VENV)"
	"$(PYTHON)" -m venv "$(VENV)"
	"$(PY)" -m pip install --upgrade pip
	"$(PY)" -m pip install -r "$(REQ)"

venv: $(PY)

clean:
	rm -rf "$(VENV)"

# ------------------------------------------------------------- env check ---
check-env:
	@test -n "$(TURN_HOST)"     || { echo "ERROR: TURN_HOST is not set";     exit 1; }
	@test -n "$(TURN_PORT)"     || { echo "ERROR: TURN_PORT is not set";     exit 1; }
	@test -n "$(TURN_TLS_PORT)" || { echo "ERROR: TURN_TLS_PORT is not set"; exit 1; }
	@test -n "$(TURN_USER)"     || { echo "ERROR: TURN_USER is not set";     exit 1; }
	@test -n "$(TURN_PASS)"     || { echo "ERROR: TURN_PASS is not set";     exit 1; }

# --------------------------------------------------------------- matrix ----
test-udp: $(PY) check-env
	@echo "==================== UDP ($(TURN_HOST):$(TURN_PORT)) ===================="
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --stun-only   -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --turn-only   -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --webrtc-only -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS)               -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)

test-tcp: $(PY) check-env
	@echo "==================== TCP ($(TURN_HOST):$(TURN_PORT)) ===================="
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --stun-only   --transport tcp -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --turn-only   --transport tcp -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --webrtc-only --transport tcp -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS)               --transport tcp -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)

test-tls: $(PY) check-env
	@echo "==================== TLS ($(TURN_HOST):$(TURN_TLS_PORT)) ===================="
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_TLS_PORT)" $(CREDS) --stun-only   --transport tls -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_TLS_PORT)" $(CREDS) --turn-only   --transport tls -t $(TIMEOUT)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_TLS_PORT)" $(CREDS) --webrtc-only --transport tls -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_TLS_PORT)" $(CREDS)               --transport tls -t $(TIMEOUT) --rate-mbps $(MATRIX_RATE) --duration $(MATRIX_DUR)
	@echo "-- TLS --insecure smoke (turn-only) --"
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_TLS_PORT)" $(CREDS) --turn-only   --transport tls --insecure -t $(TIMEOUT)

# --------------------------------------------------------------- guards ----
# Each command must be rejected by argparse / input validation with exit 2.
test-guards: $(PY) check-env
	@echo "==================== argument guards (each must exit 2) ===================="
	@echo "-- mutually exclusive --modes --"; \
	  $(RUN) -H "$(TURN_HOST)" $(CREDS) --stun-only --turn-only; \
	  [ $$? -eq 2 ] && echo "  OK" || { echo "  FAIL (expected exit 2)"; exit 1; }
	@echo "-- --rate-mbps 0 --"; \
	  $(RUN) -H "$(TURN_HOST)" $(CREDS) --rate-mbps 0; \
	  [ $$? -eq 2 ] && echo "  OK" || { echo "  FAIL (expected exit 2)"; exit 1; }
	@echo "-- --duration 0 --"; \
	  $(RUN) -H "$(TURN_HOST)" $(CREDS) --duration 0; \
	  [ $$? -eq 2 ] && echo "  OK" || { echo "  FAIL (expected exit 2)"; exit 1; }
	@echo "-- --port 70000 --"; \
	  $(RUN) -H "$(TURN_HOST)" $(CREDS) -P 70000; \
	  [ $$? -eq 2 ] && echo "  OK" || { echo "  FAIL (expected exit 2)"; exit 1; }

# ----------------------------------------------------------- speed modes ---
test-speed: $(PY) check-env
	@echo "==================== speed modes ===================="
	@echo "-- low: $(RATE_LOW) Mbps x $(DUR_LOW)s (pacing correctness; must pass) --"
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --webrtc-only --rate-mbps $(RATE_LOW) --duration $(DUR_LOW) -t $(TIMEOUT)
	@echo "-- moderate: $(RATE_MOD) Mbps x $(DUR_MOD)s (should pass on a healthy path) --"
	$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --webrtc-only --rate-mbps $(RATE_MOD) --duration $(DUR_MOD) -t $(TIMEOUT)
	@echo "-- high: $(RATE_HIGH) Mbps x $(DUR_HIGH)s (capacity probe; failure is informational) --"
	-$(RUN) -H "$(TURN_HOST)" -P "$(TURN_PORT)" $(CREDS) --webrtc-only --rate-mbps $(RATE_HIGH) --duration $(DUR_HIGH) -t $(TIMEOUT)
	@echo "(read the 'SCTP retransmissions' line above to judge channel quality)"

# ----------------------------------------------------------------- all -----
test: $(PY) check-env
	@$(MAKE) --no-print-directory test-udp
	@$(MAKE) --no-print-directory test-tcp
	@$(MAKE) --no-print-directory test-tls
	@$(MAKE) --no-print-directory test-guards
	@$(MAKE) --no-print-directory test-speed
	@echo ""
	@echo "==================== ALL SCENARIOS COMPLETE ===================="

all: test
