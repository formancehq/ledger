package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/application/restoreseed"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// Both database constructors depend on this barrier, so no Raft constructor
// can consume RESTORED before the live secondary has been installed.
type restoredReadIndexReady struct{}

func (cfg Config) readIndexDir() string {
	if cfg.ReadIndexConfig.Dir != "" {
		return cfg.ReadIndexConfig.Dir
	}

	return filepath.Join(cfg.DataDir, "read-indexes")
}

func prepareRestoredReadIndex(cfg Config, w *wal.DefaultWAL) (restoredReadIndexReady, error) {
	var ready restoredReadIndexReady
	// A crash after NewNode durably seeded its voter set can leave the old
	// RESTORED marker on disk. At that point this is a normal restart; the
	// live index may have been written by a released binary with no seed ID.
	snapshot, err := w.Snapshot()
	if err != nil {
		return ready, fmt.Errorf("reading WAL before restoring read index: %w", err)
	}
	if len(snapshot.GetMetadata().GetConfState().GetVoters()) != 0 {
		if _, err := os.Stat(filepath.Join(cfg.DataDir, "live")); errors.Is(err, os.ErrNotExist) {
			marker, err := node.ReadRestoredMarker(cfg.DataDir)
			if err != nil {
				return ready, err
			}
			if marker != nil {
				return ready, errors.New("cannot bootstrap restored data with an initialized Raft WAL and no live primary store; select the restore's fresh WAL directory")
			}
		} else if err != nil {
			return ready, err
		}

		return ready, nil
	}
	marker, err := node.ReadRestoredMarker(cfg.DataDir)
	if err != nil || marker == nil {
		return ready, err
	}
	if err := restoreseed.ValidateLocation(cfg.DataDir, cfg.readIndexDir(), filepath.Join(cfg.RaftConfig.WalDir, "etcd"), filepath.Join(cfg.RaftConfig.WalDir, "snap"), cfg.PebbleConfig.WALFailoverDir); err != nil {
		return ready, err
	}
	if marker.ReadIndexGeneration == "" {
		// A RESTORED marker published by an older release has no secondary. Give
		// its empty index a unique generation too: a stale external index with a
		// coincidentally identical cursor must never be paired with this primary.
		generation, err := restoreseed.StageCold(context.Background(), cfg.DataDir)
		if err != nil {
			return ready, err
		}
		marker.ReadIndexGeneration = generation
		if err := node.WriteRestoredMarker(cfg.DataDir, *marker); err != nil {
			return ready, err
		}
	}
	if err := restoreseed.Install(cfg.DataDir, cfg.readIndexDir(), marker.ReadIndexGeneration); err != nil {
		return ready, fmt.Errorf("installing restored read index: %w", err)
	}

	return ready, nil
}
