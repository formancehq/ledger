# Follower snapshot synchronization

When a Raft follower can no longer catch up from retained log entries, it
restores a Pebble checkpoint from a peer through the snapshot gRPC service.
The protocol is session based so multiple checkpoint files can be transferred
in parallel.

## Protocol

1. `PrepareSnapshot` waits for the serving FSM to reach the requested Raft
   index, creates a temporary Pebble checkpoint, and returns a session ID plus
   a manifest containing only each regular file's relative path and size.
2. `FetchFile` streams one manifest file in chunks. The server computes SHA-256
   while reading the file and puts the lowercase hexadecimal digest on the EOF
   response.
3. The follower writes chunks to a `.tmp` file while computing the same digest.
   It requires the exact manifest byte count and a valid, matching SHA-256
   digest before atomically renaming the temporary file.
4. `CloseSession` removes the temporary checkpoint. Session expiry provides a
   backstop when the caller disappears.

Manifest construction never reads file contents. Its cost therefore scales
with the number of checkpoint files rather than the checkpoint size. The file
contents are read and hashed exactly once, during transfer.

This protocol revision deliberately does not negotiate checksum behavior and
does not support mixed-version snapshot synchronization. Deploy it as a
coordinated, non-rolling upgrade of every node in the cluster: every peer must
run a version that sends and verifies the EOF digest before normal operation
resumes.

## Retries and staging

A transient `FetchFile` failure retries that file within the current session.
The retry rewrites its `.tmp` file from the beginning. A new session fetches
every manifest file again: without a content digest in the manifest, a file
left by an earlier checkpoint cannot be trusted from path and size alone.

The final rename is the trust boundary. Truncated content, excess content, a
missing or malformed digest, and a digest mismatch all leave the final path
untouched.

## WAL reclamation after restore

Installing a received Raft snapshot durably writes the snapshot record and
HardState, clears cached entries through the snapshot index, then releases etcd
WAL segment locks through that index. Lock release is shared with normal log
compaction and runs after the in-memory WAL mutex is released because filesystem
cleanup may be slow.

Lock release is best effort: failure is logged but cannot roll back an already
persisted snapshot or completed in-memory compaction. The background WAL purger
removes unlocked segment files.
