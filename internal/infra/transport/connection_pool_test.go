package transport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPoolConfig_SetDefaults(t *testing.T) {
	t.Parallel()

	cfg := PoolConfig{}
	cfg.SetDefaults()

	require.Equal(t, 100*time.Millisecond, cfg.BackoffBaseDelay)
	require.Equal(t, time.Second, cfg.BackoffMaxDelay)
	require.Equal(t, 1.6, cfg.BackoffMultiplier)
	require.Equal(t, 0.2, cfg.BackoffJitter)
}

func TestPoolConfig_SetDefaults_PreservesExisting(t *testing.T) {
	t.Parallel()

	cfg := PoolConfig{
		BackoffBaseDelay:  200 * time.Millisecond,
		BackoffMaxDelay:   2 * time.Second,
		BackoffMultiplier: 2.0,
		BackoffJitter:     0.5,
	}
	cfg.SetDefaults()

	require.Equal(t, 200*time.Millisecond, cfg.BackoffBaseDelay)
	require.Equal(t, 2*time.Second, cfg.BackoffMaxDelay)
	require.Equal(t, 2.0, cfg.BackoffMultiplier)
	require.Equal(t, 0.5, cfg.BackoffJitter)
}

func TestNewConnectionPool(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})
	require.NotNil(t, pool)
	require.Empty(t, pool.PeerIDs())
}

func TestConnectionPool_AddPeerAndGet(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.AddPeer(1, "localhost:9000")
	require.NoError(t, err)

	conn := pool.GetConnection(1)
	require.NotNil(t, conn)

	addr := pool.GetPeerAddress(1)
	require.Equal(t, "localhost:9000", addr)

	ids := pool.PeerIDs()
	require.Len(t, ids, 1)
	require.Contains(t, ids, uint64(1))

	require.NoError(t, pool.Close())
}

func TestConnectionPool_AddPeerIdempotent(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.AddPeer(1, "localhost:9000")
	require.NoError(t, err)

	// Same peer, same address — should be no-op
	err = pool.AddPeer(1, "localhost:9000")
	require.NoError(t, err)

	ids := pool.PeerIDs()
	require.Len(t, ids, 1)

	require.NoError(t, pool.Close())
}

func TestConnectionPool_AddPeerReplacesAddress(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.AddPeer(1, "localhost:9000")
	require.NoError(t, err)

	// Same peer, different address — should replace
	err = pool.AddPeer(1, "localhost:9001")
	require.NoError(t, err)

	addr := pool.GetPeerAddress(1)
	require.Equal(t, "localhost:9001", addr)

	require.NoError(t, pool.Close())
}

func TestConnectionPool_GetConnectionUnknownPeer(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	conn := pool.GetConnection(999)
	require.Nil(t, conn)

	addr := pool.GetPeerAddress(999)
	require.Empty(t, addr)
}

func TestConnectionPool_RemovePeer(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.AddPeer(1, "localhost:9000")
	require.NoError(t, err)

	err = pool.RemovePeer(1)
	require.NoError(t, err)

	conn := pool.GetConnection(1)
	require.Nil(t, conn)

	ids := pool.PeerIDs()
	require.Empty(t, ids)
}

func TestConnectionPool_RemovePeerNotFound(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	// Removing non-existent peer should be no-op
	err := pool.RemovePeer(999)
	require.NoError(t, err)
}

func TestConnectionPool_MultiplePeers(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	require.NoError(t, pool.AddPeer(1, "localhost:9001"))
	require.NoError(t, pool.AddPeer(2, "localhost:9002"))
	require.NoError(t, pool.AddPeer(3, "localhost:9003"))

	ids := pool.PeerIDs()
	require.Len(t, ids, 3)

	require.Equal(t, "localhost:9001", pool.GetPeerAddress(1))
	require.Equal(t, "localhost:9002", pool.GetPeerAddress(2))
	require.Equal(t, "localhost:9003", pool.GetPeerAddress(3))

	require.NoError(t, pool.Close())
}

func TestConnectionPool_Close(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	require.NoError(t, pool.AddPeer(1, "localhost:9001"))
	require.NoError(t, pool.AddPeer(2, "localhost:9002"))

	err := pool.Close()
	require.NoError(t, err)

	// After close, pool should be empty
	require.Empty(t, pool.PeerIDs())
	require.Nil(t, pool.GetConnection(1))
	require.Nil(t, pool.GetConnection(2))
}

func TestConnectionPool_RestartConnection(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	require.NoError(t, pool.AddPeer(1, "localhost:9001"))

	oldConn := pool.GetConnection(1)
	require.NotNil(t, oldConn)

	err := pool.RestartConnection(1)
	require.NoError(t, err)

	newConn := pool.GetConnection(1)
	require.NotNil(t, newConn)
	// New connection should be a different object
	require.NotSame(t, oldConn, newConn)

	require.NoError(t, pool.Close())
}

func TestDialOptions_WithCompression(t *testing.T) {
	t.Parallel()

	cfg := PoolConfig{Compression: true}
	cfg.SetDefaults()

	opts := dialOptions(insecure.NewCredentials(), cfg)
	// With compression, we get an extra option
	require.NotEmpty(t, opts)

	cfgNoComp := PoolConfig{Compression: false}
	cfgNoComp.SetDefaults()
	optsNoComp := dialOptions(insecure.NewCredentials(), cfgNoComp)

	// Compression adds one more option
	require.Greater(t, len(opts), len(optsNoComp))
}
