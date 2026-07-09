// Package raftutil provides small pointer helpers for constructing raftpb
// protobuf types. Since etcd/raft v3.7 migrated to google.golang.org/protobuf,
// all scalar enum/scalar fields are now pointers; these helpers cut down on
// boilerplate at struct-literal construction sites.
package raftutil

import "go.etcd.io/raft/v3/raftpb"

// EntryType returns a pointer to t for use in raftpb.Entry struct literals.
//
//go:fix inline
func EntryType(t raftpb.EntryType) *raftpb.EntryType { return new(t) }

// ConfChangeType returns a pointer to t for use in raftpb.ConfChangeSingle
// and raftpb.ConfChange struct literals.
//
//go:fix inline
func ConfChangeType(t raftpb.ConfChangeType) *raftpb.ConfChangeType { return new(t) }

// MessageType returns a pointer to t for use in raftpb.Message struct literals.
//
//go:fix inline
func MessageType(t raftpb.MessageType) *raftpb.MessageType { return new(t) }
