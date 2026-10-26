# Security and bug review: swpt_creditors

| | |
|---|---|
| Reviewed commit | `708bf51` (2026-10-15) |
| Review date | 2026-10-26 |
| Scope | `swpt_creditors/` (routes, procedures, models, actors, schemas), `docker/oathkeeper` |
| Method | Manual code review; findings reproduced against PostgreSQL 16 with the project's test configuration |

This document covers the issues **fixed in this pull request**, plus
review notes. Additional findings that are not fixed here have been
reported to the maintainers privately.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| SEC-1 | Medium | A transfer message with an unexpected coordinator type stops message processing | Fixed |

## SEC-1: Poison transfer message halts the consumer (Medium, fixed)

**Where:** `swpt_creditors/actors.py`, `_on_rejected_direct_transfer_signal`,
`_on_prepared_direct_transfer_signal` and
`_on_finalized_direct_transfer_signal`.

**Problem.** These handlers **raised `RuntimeError`** when a
`RejectedTransfer`, `PreparedTransfer` or `FinalizedTransfer` message
had a `coordinator_type` other than `direct`. The protocol schemas
accept any coordinator type, so such a message reaches the handler.

In `swpt_pythonlib.rabbitmq.Consumer`, an exception from
`process_message` stops the worker thread and terminates consumption.
The message is never acknowledged, so it is redelivered after the
restart, and the consumer stops again. One message therefore blocks all
further messages in the queue. These messages come from accounting
nodes, which may be run by other organizations.

`swpt_debtors` handles the same situation (an unexpected `issuing`
coordinator type) by logging an error and ignoring the message.

**Reproduction.**

```
RejectedTransfer with "coordinator_type": "unexpected"
SmpConsumer.process_message(body, props)
→ RuntimeError: Unexpected coordinator type: "unexpected"
```

**Fix.** The three handlers now log an error and ignore the message,
as in `swpt_debtors`.

**Test:** `test_consumer_unexpected_coordinator_type`.

## Review notes

- **PIN protection.** Failed attempts are recorded in their own
  transaction (`update_pin_info_helper` and `verify_pin_value`), and the
  caller raises `WrongPinValue` only afterwards. The failure counter
  therefore cannot be rolled back by the error that the wrong PIN
  causes. The consecutive and accumulated failure limits block the PIN
  as designed. All value-moving or configuration-changing endpoints
  (transfers, account config, display, exchange, PIN changes) verify
  the PIN unless the request is in PIN-reset mode.
- **Account deletion** without a PIN is only possible when the account
  is marked safe for deletion, unsafe deletion was allowed (which needs
  a PIN), or no debtor name has been set.
- **No outbound requests.** The account and debtor lookup endpoints only
  parse URIs and make no network requests, so there is no SSRF surface.
- **Transfer slugs.** `parse_transfer_slug` handles overflow and
  out-of-range values, returning 404.
- The existing lint warning `tests/test_actors.py:1 F401` predates this
  change.

## Tests

- `test_consumer_unexpected_coordinator_type`

It fails without the fix. Full suite: 124 passed, 1 skipped.
