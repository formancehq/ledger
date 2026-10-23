package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/wasm"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type WasmServiceServer struct {
	wasmpb.UnimplementedWasmServiceServer

	logger  logging.Logger
	store   *dal.Store
	node    *node.Node
	manager *wasm.Manager
}

func NewWasmServiceServer(logger logging.Logger, store *dal.Store, raftNode *node.Node, ledgerCaller wasm.LedgerCaller) *WasmServiceServer {
	server := &WasmServiceServer{logger: logger, store: store, node: raftNode}
	server.manager = wasm.NewManager(server.propose, func(format string, args ...any) {
		logger.Infof(format, args...)
	}, ledgerCaller)

	return server
}

func (s *WasmServiceServer) OnLeadershipChange(isLeader bool) {
	if isLeader {
		if modules, err := wasm.LoadModules(s.store); err == nil {
			s.manager.Replace(modules)
		} else {
			s.logger.Errorf("failed to load persisted wasm modules: %v", err)
		}
	}
	s.manager.OnLeadershipChange(isLeader)
}

func (s *WasmServiceServer) Upload(stream wasmpb.WasmService_UploadServer) error {
	if !s.node.IsLeader() {
		return status.Error(codes.Unavailable, "wasm upload requires the leader")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	moduleID := uuid.NewString()
	config := append([]byte(nil), first.GetConfig()...)
	var size uint64
	chunkIndex := uint64(0)
	if err := s.uploadChunk(stream.Context(), moduleID, config, chunkIndex, first.GetChunk(), size); err != nil {
		_ = s.deletePersisted(context.Background(), moduleID)

		return err
	}
	size += uint64(len(first.GetChunk()))
	chunkIndex++

	for {
		request, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			_ = s.deletePersisted(context.Background(), moduleID)

			return recvErr
		}
		chunk := request.GetChunk()
		if err := s.uploadChunk(stream.Context(), moduleID, config, chunkIndex, chunk, size+uint64(len(chunk))); err != nil {
			_ = s.deletePersisted(context.Background(), moduleID)

			return err
		}
		size += uint64(len(chunk))
		chunkIndex++
	}

	module := &wasm.Module{ID: moduleID, Config: config, Status: raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_RUNNING, Size: size}
	if err := s.propose(stream.Context(), &raftcmdpb.WasmModuleUpdate{
		ModuleId:  moduleID,
		Operation: raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_SET_STATUS,
		Config:    config,
		Status:    raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_RUNNING,
		Size:      size,
	}); err != nil {
		_ = s.deletePersisted(context.Background(), moduleID)

		return err
	}
	if modules, loadErr := wasm.LoadModules(s.store); loadErr == nil {
		for _, candidate := range modules {
			if candidate.ID == moduleID {
				module = candidate

				break
			}
		}
	}
	s.manager.Add(module)
	_ = s.manager.Start(moduleID)

	return stream.SendAndClose(&wasmpb.UploadWasmResponse{ModuleId: moduleID})
}

func (s *WasmServiceServer) uploadChunk(ctx context.Context, moduleID string, config []byte, index uint64, chunk []byte, size uint64) error {
	return s.propose(ctx, &raftcmdpb.WasmModuleUpdate{
		ModuleId:   moduleID,
		Operation:  raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_UPLOAD_CHUNK,
		Chunk:      append([]byte(nil), chunk...),
		Config:     append([]byte(nil), config...),
		Status:     raftcmdpb.WasmModuleStatus_WASM_MODULE_STATUS_UPLOADING,
		ChunkIndex: index,
		Size:       size,
	})
}

func (s *WasmServiceServer) List(ctx context.Context, _ *wasmpb.ListWasmModulesRequest) (*wasmpb.ListWasmModulesResponse, error) {
	modules, err := wasm.LoadModules(s.store)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response := &wasmpb.ListWasmModulesResponse{}
	for _, module := range modules {
		response.Modules = append(response.Modules, toProtoModule(module))
	}

	return response, nil
}

func (s *WasmServiceServer) Get(ctx context.Context, request *wasmpb.GetWasmModuleRequest) (*wasmpb.WasmModule, error) {
	modules, err := wasm.LoadModules(s.store)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, module := range modules {
		if module.ID == request.GetModuleId() {
			return toProtoModule(module), nil
		}
	}

	return nil, status.Error(codes.NotFound, "wasm module not found")
}

func (s *WasmServiceServer) Stop(ctx context.Context, request *wasmpb.StopWasmModuleRequest) (*wasmpb.WasmModule, error) {
	if !s.node.IsLeader() {
		return nil, status.Error(codes.Unavailable, "wasm stop requires the leader")
	}
	if err := s.manager.Stop(ctx, request.GetModuleId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	module, err := s.Get(ctx, &wasmpb.GetWasmModuleRequest{ModuleId: request.GetModuleId()})

	return module, err
}

func (s *WasmServiceServer) Delete(ctx context.Context, request *wasmpb.DeleteWasmModuleRequest) (*wasmpb.DeleteWasmModuleResponse, error) {
	if !s.node.IsLeader() {
		return nil, status.Error(codes.Unavailable, "wasm delete requires the leader")
	}
	if err := s.manager.Remove(request.GetModuleId()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := s.deletePersisted(ctx, request.GetModuleId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &wasmpb.DeleteWasmModuleResponse{}, nil
}

func (s *WasmServiceServer) deletePersisted(ctx context.Context, moduleID string) error {
	return s.propose(ctx, &raftcmdpb.WasmModuleUpdate{
		ModuleId:  moduleID,
		Operation: raftcmdpb.WasmModuleOperation_WASM_MODULE_OPERATION_DELETE,
	})
}

func (s *WasmServiceServer) propose(ctx context.Context, update *raftcmdpb.WasmModuleUpdate) error {
	// Module lifecycle state is a Ledger-owned system proposal. The public WASM
	// RPC is intentionally unauthenticated in this MVP, so attach the explicit
	// system attribution before entering the normal admission path.
	ctx = internalauth.WithSystemActor(ctx, commands.ComponentWasm)
	if !s.node.IsLeader() {
		return node.ErrNotLeader
	}
	command := commands.NewCommand()
	command.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentWasm)
	command.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_WasmModule{WasmModule: update}}}
	data, err := command.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal wasm command: %w", err)
	}
	proposal := node.NewProposal(command.GetId(), data)
	future, err := s.node.Propose(ctx, proposal)
	if err != nil {
		return err
	}
	if _, err := proposal.Wait(ctx); err != nil {
		return err
	}
	if _, err := future.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func toProtoModule(module *wasm.Module) *wasmpb.WasmModule {
	return &wasmpb.WasmModule{
		ModuleId:  module.ID,
		Config:    append([]byte(nil), module.Config...),
		Status:    wasmpb.WasmModuleStatus(module.Status),
		LastError: module.LastError,
		Size:      module.Size,
	}
}
