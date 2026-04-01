package internal

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

const (
	FaultPausingDuration              int64 = 60
	AvailabilityAssertionsSafetyMargin int64 = 5
)

// NewEtcdClient creates a new etcd client using ETCD_ENDPOINTS or defaults.
func NewEtcdClient() *etcd.Client {
	endpoints := []string{
		"http://etcd-0.etcd.default.svc.cluster.local:2379",
		"http://etcd-1.etcd.default.svc.cluster.local:2379",
		"http://etcd-2.etcd.default.svc.cluster.local:2379",
	}
	if env := os.Getenv("ETCD_ENDPOINTS"); env != "" {
		endpoints = strings.Split(env, ",")
	}
	client, err := etcd.New(etcd.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return client
}

// FaultsActive returns true if Antithesis fault injection is currently active,
// as determined by the /last_pause key in etcd.
func FaultsActive(ctx context.Context) bool {
	etcdClient := NewEtcdClient()
	defer etcdClient.Close()

	lastPause, err := etcdClient.Get(ctx, "/last_pause")
	if err != nil {
		return true
	}
	if len(lastPause.Kvs) == 0 {
		return true
	}
	lastPauseUnix, err := strconv.ParseInt(string(lastPause.Kvs[0].Value), 10, 64)
	if err != nil {
		return true
	}
	sinceLastPause := time.Now().Unix() - lastPauseUnix
	return sinceLastPause < AvailabilityAssertionsSafetyMargin ||
		sinceLastPause > FaultPausingDuration-AvailabilityAssertionsSafetyMargin
}
