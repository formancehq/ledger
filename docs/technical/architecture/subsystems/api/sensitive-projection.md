# Sensitive protobuf projection

`common.sensitive` is a protobuf field option for values that must not appear
verbatim in a public view. The generic `internal/pkg/sensitive.Clone` helper
creates a deep copy before walking its descriptors. It replaces non-empty
annotated strings with `[redacted]` and clears annotated fields of other kinds,
including whole signed envelopes. It recursively visits nested messages,
oneofs, repeated messages and message-valued maps, and removes unknown wire
fields whose confidentiality cannot be established from the schema.

The source remains unchanged, including authoritative signed bytes. Never use
this helper to persist or sign records: a public projection is not audit evidence.
Annotations do not change serialization by themselves; callers must explicitly
select the projection at a read boundary. The helper does not parse credentials
embedded inside URL or DSN strings. Those require a structured model before
individual secret components can be annotated.

This mechanism is a foundation. It does not by itself wire public endpoints or
claim that existing configuration responses are completely redacted. Input-only
messages need no annotation for this purpose. A type shared between input and
output can carry annotations because projection acts only on the explicit copy.

Adding the field option and helper does not change the service wire contract;
the service protocol revision remains unchanged.

Sink status messages are already sanitized by their producing adaptor and remain
visible through this projector. `SinkError.message` deliberately has no sensitive
annotation: masking the entire diagnostic would prevent operators from diagnosing
failures. This contract does not extend to mirror or audit diagnostics, whose
sensitive annotations remain independent.
