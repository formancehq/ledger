package wasm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

var ErrModuleRunning = errors.New("wasm module is running")

type Module struct {
	ID        string
	Wasm      []byte
	Config    []byte
	Status    raftcmdpb.WasmModuleStatus
	LastError string
	Size      uint64
}

type Proposer func(context.Context, *raftcmdpb.WasmModuleUpdate) error
type Logf func(string, ...any)

type Manager struct {
	mu        sync.Mutex
	modules   map[string]*Module
	instances map[string]context.CancelFunc
	propose   Proposer
	logger    Logf
	runtime   *Runtime
	leader    bool
}

func NewManager(propose Proposer, logger Logf, caller LedgerCaller) *Manager {
	return &Manager{
		modules:   make(map[string]*Module),
		instances: make(map[string]context.CancelFunc),
		propose:   propose,
		logger:    logger,
		runtime: NewRuntime(caller, func(level uint32, text string) {
			if logger != nil {
				logger("wasm[%d] %s", level, text)
			}
		}),
	}
}

func (m *Manager) Replace(modules []*Module) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.modules = make(map[string]*Module, len(modules))
	for _, module := range modules {
		clone := *module
		clone.Wasm = append([]byte(nil), module.Wasm...)
		clone.Config = append([]byte(nil), module.Config...)
		m.modules[module.ID] = &clone
	}
}

func (m *Manager) Add(module *Module) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *module
	clone.Wasm = append([]byte(nil), module.Wasm...)
	clone.Config = append([]byte(nil), module.Config...)
	m.modules[module.ID] = &clone
}

func (m *Manager) Remove(moduleID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, running := m.instances[moduleID]; running {
		return ErrModuleRunning
	}
	delete(m.modules, moduleID)

	return nil
}

func (m *Manager) List() []*Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*Module, 0, len(m.modules))
	for _, module := range m.modules {
		clone := *module
		clone.Wasm = append([]byte(nil), module.Wasm...)
		clone.Config = append([]byte(nil), module.Config...)
		result = append(result, &clone)
	}

	return result
}

func (m *Manager) OnLeadershipChange(isLeader bool) {
	m.mu.Lock()
	wasLeader := m.leader
	m.leader = isLeader
	if !isLeader {
		for _, cancel := range m.instances {
			cancel()
		}
		m.instances = make(map[string]context.CancelFunc)
	}
	m.mu.Unlock()

	if isLeader && !wasLeader {
		for _, module := range m.List() {
			if module.Status == raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_RUNNING {
				m.start(module)
			}
		}
	}
}

func (m *Manager) Start(moduleID string) error {
	m.mu.Lock()
	module, ok := m.modules[moduleID]
	leader := m.leader
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("wasm module %q not found", moduleID)
	}
	if !leader {
		return node.ErrNotLeader
	}
	m.start(module)

	return nil
}

func (m *Manager) start(module *Module) {
	m.mu.Lock()
	if _, running := m.instances[module.ID]; running || !m.leader {
		m.mu.Unlock()

		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.instances[module.ID] = cancel
	m.mu.Unlock()

	go func() {
		err := m.runtime.Run(ctx, module.Wasm, module.Config)
		m.mu.Lock()
		delete(m.instances, module.ID)
		stillLeader := m.leader
		m.mu.Unlock()
		if !stillLeader || errors.Is(err, context.Canceled) {
			return
		}

		status := raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_STOPPED
		message := ""
		if err != nil {
			status = raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_ERROR
			message = err.Error()
		}
		if m.logger != nil && err != nil {
			m.logger("wasm module %s stopped with error: %v", module.ID, err)
		}
		_ = m.persistStatus(module, status, message)
	}()
}

func (m *Manager) Stop(ctx context.Context, moduleID string) error {
	m.mu.Lock()
	module, ok := m.modules[moduleID]
	cancel, running := m.instances[moduleID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("wasm module %q not found", moduleID)
	}
	if !running {
		return m.persistStatus(module, raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_STOPPED, "")
	}
	cancel()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		_, stillRunning := m.instances[moduleID]
		m.mu.Unlock()
		if !stillRunning {
			return m.persistStatus(module, raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_STOPPED, "")
		}
		select {
		case <-deadline.C:
			return m.persistStatus(module, raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_ERROR, "forced stop after 5s")
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) persistStatus(module *Module, status raftcmdpb.WasmModuleStatus, message string) error {
	if m.propose == nil {
		return errors.New("wasm proposer is not configured")
	}
	update := &raftcmdpb.WasmModuleUpdate{
		ModuleId:  module.ID,
		Operation: raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_SET_STATUS,
		Config:    append([]byte(nil), module.Config...),
		Status:    status,
		Error:     message,
		Size:      module.Size,
	}
	if err := m.propose(context.Background(), update); err != nil {
		return err
	}
	m.mu.Lock()
	module.Status = status
	module.LastError = message
	m.mu.Unlock()

	return nil
}
