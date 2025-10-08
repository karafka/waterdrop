# Fatal Error Recovery Integration Tests

This directory contains integration tests for WaterDrop's automatic fatal error recovery feature for idempotent and transactional producers.

## Overview

These tests verify the behavior of producers when they encounter fatal librdkafka errors:

- **fencing_no_reload_spec.rb** - Verifies that producer fencing errors do NOT trigger reload attempts, as fencing is an unrecoverable state
- **idempotent_reload_spec.rb** - Tests that idempotent producers with reload enabled can recover from fatal errors
- **transaction_reload_spec.rb** - Tests that transactional producers with reload enabled are configured correctly
- **reload_disabled_spec.rb** - Verifies that the reload feature can be disabled

## Prerequisites

- Kafka broker running and accessible
- Bootstrap servers configured via `BOOTSTRAP_SERVERS` environment variable (defaults to `127.0.0.1:9092`)

## Running the Tests

Run individual tests:

```bash
bundle exec ruby spec/integrations/fatal_error_recovery/fencing_no_reload_spec.rb
bundle exec ruby spec/integrations/fatal_error_recovery/idempotent_reload_spec.rb
bundle exec ruby spec/integrations/fatal_error_recovery/transaction_reload_spec.rb
bundle exec ruby spec/integrations/fatal_error_recovery/reload_disabled_spec.rb
```

Or with custom bootstrap servers:

```bash
BOOTSTRAP_SERVERS=localhost:9092 bundle exec ruby spec/integrations/fatal_error_recovery/fencing_no_reload_spec.rb
```

Run all tests in the suite:

```bash
./bin/integrations fatal_error_recovery
```

## Test Descriptions

### Fencing No Reload Test

Creates two transactional producers with the same `transactional.id`. The second producer fences the first one, and we verify that:
1. The fenced producer raises a fencing error
2. The producer does NOT attempt to reload (as fencing is unrecoverable)

### Idempotent Reload Test

Creates an idempotent producer with `reload_on_idempotent_fatal_error: true` and uses monkey patching to inject a fatal error on the first produce call. This verifies:
1. The producer encounters a fatal error
2. The producer automatically reloads (1 reload event occurs)
3. Subsequent produce calls succeed after reload
4. All 5 messages are successfully produced

### Transaction Reload Test

Creates a transactional producer with `reload_on_transaction_fatal_error: true` and verifies:
1. The configuration is set correctly
2. The producer can execute transactions successfully
3. No reload events occur during normal operation

Note: Testing actual transactional fatal error recovery with reload is complex due to the transaction state machine. The fencing test covers the actual fatal error scenario.

### Reload Disabled Test

Creates an idempotent producer with `reload_on_idempotent_fatal_error: false` and verifies that no reload events occur during normal operation.

## Exit Codes

Each test exits with:
- `0` - Test passed
- `1` - Test failed

## Notes

These are integration tests that require a real Kafka broker. They test the actual behavior of producers with real Kafka connections, not mocked scenarios.

The idempotent reload test uses monkey patching to inject a simulated fatal error to demonstrate the reload mechanism in action. The transaction reload test focuses on configuration validation and normal operation, while the fencing test demonstrates the real-world scenario where reload should NOT occur.
