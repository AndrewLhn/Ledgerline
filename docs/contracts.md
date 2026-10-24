# Event contract governance

## Rules

1. Contract files are immutable. Once `contracts/kafka_event_v<N>.schema.json`
   is merged, it does not change. CI enforces this: every pull request is
   compared against the base branch (scripts/check_contract_compat.py).
2. Schemas are closed (`additionalProperties: false`), so even adding an
   optional field is a new version: a validator pinned to the old file would
   reject the extended payload.
3. Readers support every version registered in `CONTRACT_FILES`
   (dags/ledgerline_contract.py). Writers produce exactly one version.
4. Each file must be self-consistent: valid Draft 2020-12 with a
   `schema_version` const equal to the N in its filename. CI checks this on
   every run, not only on contract changes.

## Bumping a version

1. Copy the newest file to `kafka_event_v<N+1>.schema.json`, apply the change
   there, and update the `schema_version` const and title.
2. Register the version in `CONTRACT_FILES`; keep older versions registered.
3. Add a producer scenario and contract tests for the new version.
4. Transformations: staging carries `schema_version`; marts extract new fields
   (null for older events); add the version to the `accepted_values` tests.
5. Write an ADR recording why the change could not be avoided and how
   producers migrate. ADR-008 and the v1→v2 payment `method` field are the
   worked example.

## Deprecation

A version is deprecated by producers ceasing to emit it. Readers keep
validating it while raw data or in-flight messages may contain it. Removing
reader support is itself a versioned decision (ADR) with a cut-off date and a
staging filter for stragglers.

## Exceptions

Before the first external consumer exists, editing a contract in place is
allowed. Such a PR carries the `contract-breaking` label, which makes CI skip
the immutability check. The label is a visible, reviewed acknowledgement, not
a loophole: the v1 file was edited under this rule pre-release (money moved to
integer minor units, the demo `id` field removed).
