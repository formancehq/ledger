package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

const forceRemoveMarkerName = "FORCE_REMOVE_PENDING"

type forceRemoveMarker struct {
	nodeID     uint64
	instanceID []byte
}

func writeForceRemoveMarker(dir string, nodeID uint64, instanceID []byte) error {
	if err := membership.ValidateInstanceID(instanceID); err != nil {
		return err
	}
	if dir == "" {
		return errors.New("force-remove marker requires WAL directory")
	}
	file, err := os.CreateTemp(dir, ".force-remove-*")
	if err != nil {
		return err
	}
	// A successful rename removes the temporary name; on earlier failure,
	// cleanup is best effort and cannot make an unsafe marker authoritative.
	defer func() { _ = os.Remove(file.Name()) }()
	var data [24]byte
	binary.BigEndian.PutUint64(data[:8], nodeID)
	copy(data[8:], instanceID)
	if _, err := file.Write(data[:]); err != nil {
		_ = file.Close()

		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()

		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(dir, forceRemoveMarkerName)); err != nil {
		return err
	}

	return syncForceRemoveMarkerDir(dir)
}

func readForceRemoveMarker(dir string) (*forceRemoveMarker, error) {
	if dir == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, forceRemoveMarkerName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) != 24 {
		return nil, fmt.Errorf("invalid force-remove marker length %d", len(data))
	}
	marker := &forceRemoveMarker{nodeID: binary.BigEndian.Uint64(data[:8]), instanceID: data[8:]}
	if err := membership.ValidateInstanceID(marker.instanceID); err != nil {
		return nil, fmt.Errorf("invalid force-remove marker identity: %w", err)
	}

	return marker, nil
}

func clearForceRemoveMarker(dir string) error {
	if err := os.Remove(filepath.Join(dir, forceRemoveMarkerName)); err != nil && !os.IsNotExist(err) {
		return err
	}

	return syncForceRemoveMarkerDir(dir)
}

func syncForceRemoveMarkerDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	// Sync is the durability check; closing this read-only directory handle
	// cannot change whether the marker rename or removal was persisted.
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func recoverForceRemoveMarker(dir string, cs *raftpb.ConfState, peers *membership.Membership) error {
	marker, err := readForceRemoveMarker(dir)
	if err != nil {
		return fmt.Errorf("reading force-remove marker: %w", err)
	}
	if marker == nil {
		return nil
	}
	if !confStateContainsNode(cs, marker.nodeID) {
		if err := peers.UnregisterAndBlacklist(marker.nodeID, marker.instanceID, 0); err != nil {
			return fmt.Errorf("repairing force-remove tombstone: %w", err)
		}
	}
	if err := clearForceRemoveMarker(dir); err != nil {
		return fmt.Errorf("clearing force-remove marker: %w", err)
	}

	return nil
}
