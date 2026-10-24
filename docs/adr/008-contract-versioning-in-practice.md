# ADR 008: Contract versioning in practice

- Status: Accepted
- Date: 2026-10-24

## Context

The event schema declared `schema_version` from day one but only version 1
existed, and the validator never looked at the field: "versioned events" was a
claim, not a capability. Payment capture now needs a `method` field
(card/wallet/bank_transfer), which is required for v2 producers and must not
break v1 messages already flowing or stored.

## Options considered

- One file with `oneOf` per version: single artifact, but validation errors
  degrade to "matches none of the schemas", and every change rewrites the
  shared file (no immutability).
- Schema registry (Confluent/Apicurio): wire compatibility rules for free,
  but a new stateful service in a stack that deliberately has none, and JSON
  Schema support there is second-class to Avro.
- One immutable file per version, code-side dispatch on `schema_version`:
  boring, explicit support matrix, artifacts that can be frozen and audited.

## Decision

Per-version immutable schema files
(`contracts/kafka_event_v<N>.schema.json`), a `CONTRACT_FILES` registry in
the contract module, and validation dispatched on the event's
`schema_version` with a clear error for unsupported versions. Because schemas
are closed, any change — even additive — ships as a new version. CI enforces
immutability and self-consistency (scripts/check_contract_compat.py);
pre-release exceptions require the `contract-breaking` PR label
(docs/contracts.md).

v2 demonstrates the mechanics: `payment_captured` gains a required `method`;
all other events are byte-identical to v1 apart from the version const.

## Consequences

Readers accept versions 1 and 2; producers pick one. Version files duplicate
the shared envelope — a registry would factor that out, at the cost of the
service. Mart extraction treats new v2 fields as nullable, so v1 history
stays queryable without backfills. Deprecation is a documented process, not
an event: producers migrate first, reader support is removed last, by ADR.
