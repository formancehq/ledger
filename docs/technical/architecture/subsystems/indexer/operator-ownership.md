# Operator index attribution from audit

## Contract and decision (EN-2002)

For a managed `Ledger.spec.indexes`, the operator must recover indexes it created
after a controller restart or a failed Kubernetes status write. Existing external
indexes must not be adopted. An absent spec remains unmanaged; an explicitly
empty spec removes attributable indexes.

The supported operating contract is that operator-created indexes are managed
through the Kubernetes spec. Manually deleting or replacing those indexes is
outside that contract. The accepted EN-2002 contract permits the residual race where an
external replacement occurs after attribution is checked but before an
unconditional drop commits. This implementation does not claim to prevent that
race. It avoids adopting external indexes and detects replacements already
visible when attribution is checked.

EN-2009 makes fresh `CreateIndex` calls strict: an existing index rejects before
any registry write or creation log. This lets an audited successful creation
prove an actual creation. Attribution uses existing audit and index fields;
there is no persisted owner, creation-owner argument, or conditional drop field.

## Evidence and recovery

Each operator creation uses its own batch and a fresh idempotency key beginning
with `ledger-operator/index/<Ledger CR UID>/`. The suffix identifies a single
attempt. A recreated CR gets a different UID; names and spec generations are
not ownership identities. The marker is cooperative attribution, not an
authentication credential.

`ledgerctl indexes list --creation-key-prefix` selects current registry rows
whose creation is supported by a matching audit entry. Evidence must describe
one successful creation order for the same ledger and canonical index ID,
with a fresh nonzero log sequence inside the successful log range. The audit
timestamp must equal the index's existing `createdAt`. Failed requests, replay
outcomes without a fresh creation, unrelated orders and multi-order batches
cannot establish attribution.

The FSM's effective timestamp advances strictly between proposals. The dedicated
creation batch matters because multiple orders in one proposal share a date.
A later deletion and recreation changes the observed date; schema retyping
preserves it. Date equality is used with verified singleton creation evidence,
not as a universal identity for arbitrary orders inside a batch.

The CLI traverses audit pages and fetches item details where needed. A failed
read does not authorize a deletion or return partial attribution as success.
Recovery requires accessible audit history. Audit history is permanent primary
state under the incremental restore contract; this command does not invent
evidence if history cannot be read. Full scans have a cost proportional to the
available audit history and remain bounded by the command timeout.

## Mutation order and interruption

1. List current indexes for desired-set reconciliation, then derive attribution
   from current registry rows and audited creation evidence. Reconstruct the
   in-memory `status.appliedIndexes` observation from that evidence.
2. Reconcile metadata declarations and create missing indexes in separate
   identified batches. Ledger commits the index and audit evidence before the
   CLI call completes. Drop attributable undesired indexes unconditionally.
3. Persist the resulting observation in Kubernetes status at the end of the
   controller pass. A successful command may therefore outlive a failed status
   update or a lost command response.

After interruption, the next pass reconstructs attribution from Ledger instead
of relying on the stale status. Strict-create conflicts are surfaced and the
next pass relists; they never establish ownership. Fresh attempt keys avoid
reusing an old successful outcome after deliberate deletion. Ordinary batch
idempotency and its TTL are unchanged.

The final verification/drop interval is not atomic. Under the accepted contract,
the operator can delete an external replacement made in that interval. A future
stronger contract would require an atomic service precondition; rereading the
audit alone cannot close the interval.

## Restore and verification

Full checkpoints retain audit entries, audit items and index creation dates.
Nonempty incremental exports retain the audit rows, while registry replay
reconstructs creation dates from creation logs. No new persistence format or
restore projection is introduced. Restored evidence is usable by the same CR
UID; a different UID does not adopt it.

Regression coverage includes real CLI/service creation followed by a one-shot
status failure, fresh-controller recovery and spec withdrawal; lost responses;
external existing/concurrent creations; known replacements; new CR UIDs; audit
pagination and invalid evidence. Restore coverage spans a checkpoint and a
nonempty delta and verifies that audit identity and creation dates still match.
