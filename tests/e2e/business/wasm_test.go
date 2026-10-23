//go:build e2e

package business

import (
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WASM modules", Ordered, func() {
	It("uploads, persists, stops and deletes a module", func() {
		wasmPath := filepath.Join("..", "..", "..", "build", "wasm", "dummy.wasm")
		wasmBytes, err := os.ReadFile(wasmPath)
		if os.IsNotExist(err) {
			Skip("build/wasm/dummy.wasm is not built; run just wasm-dummy")
		}
		Expect(err).To(Succeed())

		stream, err := sharedWasmClient.Upload(sharedCtx)
		Expect(err).To(Succeed())
		midpoint := len(wasmBytes) / 2
		Expect(stream.Send(&wasmpb.UploadWasmRequest{Config: []byte(`{"ledger":"wasm-e2e","poll_interval_ms":100}`), Chunk: wasmBytes[:midpoint]})).To(Succeed())
		Expect(stream.Send(&wasmpb.UploadWasmRequest{Chunk: wasmBytes[midpoint:]})).To(Succeed())
		response, err := stream.CloseAndRecv()
		Expect(err).To(Succeed())
		Expect(response.GetModuleId()).NotTo(BeEmpty())

		module, err := sharedWasmClient.Get(sharedCtx, &wasmpb.GetWasmModuleRequest{ModuleId: response.GetModuleId()})
		Expect(err).To(Succeed())
		Expect(module.GetSize()).To(BeNumerically(">", 0))

		_, err = sharedWasmClient.Stop(sharedCtx, &wasmpb.StopWasmModuleRequest{ModuleId: response.GetModuleId()})
		Expect(err).To(Succeed())
		_, err = sharedWasmClient.Delete(sharedCtx, &wasmpb.DeleteWasmModuleRequest{ModuleId: response.GetModuleId()})
		Expect(err).To(Succeed())

		list, err := sharedWasmClient.List(sharedCtx, &wasmpb.ListWasmModulesRequest{})
		Expect(err).To(Succeed())
		for _, candidate := range list.GetModules() {
			Expect(candidate.GetModuleId()).NotTo(Equal(response.GetModuleId()))
		}
	})
})
