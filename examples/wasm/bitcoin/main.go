//go:build wasip1

// Command bitcoin is a self-contained WASI module that polls Bitcoin Core and
// submits atomic Ledger Apply batches through the generic runtime imports.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/crypto/ripemd160"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
)

//go:wasmimport ledger call
func ledgerCall(requestPtr, requestLen, responsePtr, responseCap uint32) uint64

//go:wasmimport ledger log
func ledgerLog(level, textPtr, textLen uint32)

//go:wasmimport ledger ready
func ledgerReady()

//go:wasmimport net connect
func netConnect(hostPtr, hostLen, port uint32) uint32

//go:wasmimport net read
func netRead(handle, bufferPtr, bufferCap uint32) uint32

//go:wasmimport net write
func netWrite(handle, bufferPtr, bufferLen uint32) uint32

//go:wasmimport net request
func netRequest(hostPtr, hostLen, port, requestPtr, requestLen uint32) uint32

//go:wasmimport net poll
func netPoll(handle uint32) uint32

//go:wasmimport net response_size
func netResponseSize(handle uint32) uint32

//go:wasmimport net response_read
func netResponseRead(handle, bufferPtr, bufferCap uint32) uint32

//go:wasmimport net close
func netClose(handle uint32) uint32

type config struct {
	RPCHost      string `json:"rpc_host"`
	RPCPort      uint32 `json:"rpc_port"`
	RPCUser      string `json:"rpc_user"`
	RPCPassword  string `json:"rpc_password"`
	Ledger       string `json:"ledger"`
	StartHeight  uint64 `json:"start_height"`
	PollInterval uint64 `json:"poll_interval_ms"`
	AddressPref  string `json:"address_prefix"`
}

var (
	moduleConfig   config
	moduleHeight   uint64
	ledgerResponse = make([]byte, 4<<20)
)

//go:wasmexport alloc
func alloc(size uint32) uint32 {
	if size == 0 {
		return 0
	}
	b := make([]byte, size)
	return uint32(uintptr(unsafe.Pointer(&b[0])))
}

//go:wasmexport dealloc
func dealloc(_ uint32, _ uint32) {}

//go:wasmexport init
func initModule(ptr, length uint32) uint32 {
	if err := json.Unmarshal(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length), &moduleConfig); err != nil {
		logf(3, "invalid bitcoin config: %v", err)
		return 1
	}
	if moduleConfig.RPCPort == 0 {
		moduleConfig.RPCPort = 8332
	}
	if moduleConfig.Ledger == "" {
		moduleConfig.Ledger = "bitcoin-mainnet"
	}
	if moduleConfig.AddressPref == "" {
		moduleConfig.AddressPref = "btc"
	}
	moduleHeight = moduleConfig.StartHeight
	if restored, err := restoreHeight(); err == nil && restored > moduleHeight {
		moduleHeight = restored
	}
	return 0
}

func main() {
	ledgerReady()
	for {
		time.Sleep(time.Hour)
	}
}

//go:wasmexport run
func run() uint32 {
	var current *preparedBlock
	prefetched := make(map[uint64]chan preparationResult)
	for {
		best, err := rpcUint("getblockcount")
		if err == nil && current == nil && moduleHeight <= best {
			current, err = prepareMeasured(moduleHeight)
			if err != nil {
				logf(2, "bitcoin block %d: %v", moduleHeight, err)
			}
			if current != nil && current.height != moduleHeight {
				current = nil
			}
		}
		if current != nil {
			// Prepare a small bounded window concurrently while the Ledger applies
			// the current block. Apply itself remains strictly ordered.
			const prefetchDepth = 4
			for height := current.height + 1; height <= best && height <= current.height+prefetchDepth; height++ {
				if _, ok := prefetched[height]; !ok {
					prefetched[height] = launchPrefetch(height)
				}
			}

			var beforeApply runtime.MemStats
			runtime.ReadMemStats(&beforeApply)
			applyStarted := time.Now()
			applyErr := applyPrepared(current)
			var afterApply runtime.MemStats
			runtime.ReadMemStats(&afterApply)
			nextHeight := current.height + 1
			var preparedNext *preparedBlock
			if next, ok := prefetched[nextHeight]; ok {
				result := <-next
				preparedNext = result.block
				delete(prefetched, nextHeight)
				if result.err != nil {
					logf(2, "bitcoin block %d: %v", nextHeight, result.err)
				}
			}
			if applyErr != nil {
				logf(2, "bitcoin apply block %d: %v", moduleHeight, applyErr)
				drainPrefetch(prefetched)
				prefetched = make(map[uint64]chan preparationResult)
				current = nil
			} else {
				moduleHeight++
				if moduleHeight%100 == 0 {
					var stats runtime.MemStats
					runtime.ReadMemStats(&stats)
					logf(0, "bitcoin ingestion: next_height=%d transactions=%d prepare_duration_ms=%d apply_duration_ms=%d prepare_alloc_bytes=%d apply_alloc_bytes=%d heap_alloc_bytes=%d heap_inuse_bytes=%d heap_sys_bytes=%d gc_cycles=%d", moduleHeight, len(current.block.Tx), current.prepareDuration.Milliseconds(), time.Since(applyStarted).Milliseconds(), current.prepareAlloc, afterApply.TotalAlloc-beforeApply.TotalAlloc, stats.HeapAlloc, stats.HeapInuse, stats.HeapSys, stats.NumGC)
				}
				if preparedNext != nil && preparedNext.height == moduleHeight {
					current = preparedNext
				} else {
					drainPrefetch(prefetched)
					prefetched = make(map[uint64]chan preparationResult)
					current = nil
				}
			}
			continue
		} else if err != nil {
			logf(2, "bitcoin getblockcount: %v", err)
		}
		time.Sleep(time.Duration(pollInterval()) * time.Millisecond)
	}
}

func pollInterval() uint64 {
	if moduleConfig.PollInterval == 0 {
		return 30_000
	}
	return moduleConfig.PollInterval
}

type rpcEnvelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func rpcUint(method string) (uint64, error) {
	raw, err := rpc(method)
	if err != nil {
		return 0, err
	}
	var value uint64
	err = json.Unmarshal(raw, &value)
	return value, err
}

type block struct {
	Hash string `json:"hash"`
	Time int64  `json:"time"`
	Tx   []tx   `json:"tx"`
}
type tx struct {
	TxID string `json:"txid"`
	Vin  []vin  `json:"vin"`
	Vout []vout `json:"vout"`
}
type vin struct {
	Coinbase string   `json:"coinbase"`
	Prevout  *prevout `json:"prevout"`
}
type prevout struct {
	Value  json.Number `json:"value"`
	Script script      `json:"scriptPubKey"`
}
type vout struct {
	Value  json.Number `json:"value"`
	Script script      `json:"scriptPubKey"`
}
type script struct {
	Address string `json:"address"`
	Hex     string `json:"hex"`
}

type preparedBlock struct {
	height          uint64
	block           *block
	payload         []byte
	prepareDuration time.Duration
	prepareAlloc    uint64
}

type preparationResult struct {
	block *preparedBlock
	err   error
}

func prepareBlock(height uint64) (*preparedBlock, error) {
	started := time.Now()
	b, err := fetchBlock(height)
	if err != nil {
		return nil, err
	}
	payload, err := buildApplyPayload(height, b)
	if err != nil {
		return nil, err
	}
	return &preparedBlock{height: height, block: b, payload: payload, prepareDuration: time.Since(started)}, nil
}

func prepareMeasured(height uint64) (*preparedBlock, error) {
	started := time.Now()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	prepared, err := prepareBlock(height)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if prepared != nil {
		prepared.prepareAlloc = after.TotalAlloc - before.TotalAlloc
		prepared.prepareDuration = time.Since(started)
	}
	return prepared, err
}

func launchPrefetch(height uint64) chan preparationResult {
	result := make(chan preparationResult, 1)
	go func() {
		prepared, err := prepareMeasured(height)
		result <- preparationResult{block: prepared, err: err}
	}()
	return result
}

func drainPrefetch(prefetched map[uint64]chan preparationResult) {
	for _, result := range prefetched {
		<-result
	}
}

func fetchBlock(height uint64) (*block, error) {
	hashRaw, err := rpc("getblockhash", height)
	if err != nil {
		return nil, err
	}
	var hash string
	if err = json.Unmarshal(hashRaw, &hash); err != nil {
		return nil, err
	}
	raw, err := rpc("getblock", hash, 3)
	if err != nil {
		return nil, err
	}
	var result block
	err = json.Unmarshal(raw, &result)
	return &result, err
}

func buildApplyPayload(height uint64, b *block) ([]byte, error) {
	var requests []*servicepb.Request
	var fees []feeEntry
	var coinbase *tx
	for i := range b.Tx {
		source := b.Tx[i]
		if len(source.Vin) > 0 && source.Vin[0].Coinbase != "" {
			coinbase = &source
			continue
		}
		postings, metadata, txFees, err := mapRegular(height, source, b)
		if err != nil {
			return nil, err
		}
		fees = append(fees, txFees...)
		requests = append(requests, transactionRequest(height, postings, metadata, source.TxID, b.Time, nil))
	}
	if coinbase != nil {
		postings, metadata, accountMetadata, err := mapCoinbase(height, *coinbase, b, fees)
		if err != nil {
			return nil, err
		}
		requests = append(requests, transactionRequest(height, postings, metadata, coinbase.TxID, b.Time, accountMetadata))
	}
	requests = append(requests, &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{
		Ledger: moduleConfig.Ledger,
		Metadata: map[string]*commonpb.MetadataValue{
			"bitcoin_next_height": metadataUint(height + 1),
		},
	}}})
	if len(requests) == 0 {
		return nil, nil
	}
	apply := &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: &servicepb.ApplyBatch{Requests: requests, IdempotencyKey: fmt.Sprintf("bitcoin:block:%d", height)}}, SkipResponse: true}
	payload, err := apply.MarshalVT()
	if err != nil {
		return nil, err
	}
	request, err := (&wasmpb.LedgerCallRequest{Method: "Apply", Payload: payload}).MarshalVT()
	if err != nil {
		return nil, err
	}
	return request, nil
}

func applyPrepared(prepared *preparedBlock) error {
	request := prepared.payload
	response := ledgerResponse
	result := ledgerCall(uint32(uintptr(unsafe.Pointer(&request[0]))), uint32(len(request)), uint32(uintptr(unsafe.Pointer(&response[0]))), uint32(len(response)))
	if result == ^uint64(0) || result > uint64(len(response)) {
		return fmt.Errorf("ledger.call failed")
	}
	var envelope wasmpb.LedgerCallResponse
	if err := envelope.UnmarshalVT(response[:result]); err != nil {
		return err
	}
	if envelope.GetError() != "" {
		return fmt.Errorf("ledger apply: %s", envelope.GetError())
	}
	return nil
}

func restoreHeight() (uint64, error) {
	request, err := (&wasmpb.LedgerCallRequest{Method: "GetLedger", Payload: mustMarshalVT(&servicepb.GetLedgerRequest{Ledger: moduleConfig.Ledger})}).MarshalVT()
	if err != nil {
		return 0, err
	}
	response := make([]byte, 1<<20)
	result := ledgerCall(uint32(uintptr(unsafe.Pointer(&request[0]))), uint32(len(request)), uint32(uintptr(unsafe.Pointer(&response[0]))), uint32(len(response)))
	if result == ^uint64(0) || result > uint64(len(response)) {
		return 0, fmt.Errorf("ledger.call failed")
	}
	var envelope wasmpb.LedgerCallResponse
	if err := envelope.UnmarshalVT(response[:result]); err != nil {
		return 0, err
	}
	if envelope.GetError() != "" {
		return 0, fmt.Errorf("ledger get: %s", envelope.GetError())
	}
	var info commonpb.LedgerInfo
	if err := info.UnmarshalVT(envelope.GetPayload()); err != nil {
		return 0, err
	}
	if value := info.GetMetadata()["bitcoin_next_height"]; value != nil {
		return value.GetUintValue(), nil
	}
	return 0, nil
}

func mustMarshalVT(message interface{ MarshalVT() ([]byte, error) }) []byte {
	data, _ := message.MarshalVT()
	return data
}

type feeEntry struct {
	account string
	amount  uint64
}

func transactionRequest(height uint64, postings []*commonpb.Posting, metadata map[string]*commonpb.MetadataValue, txID string, blockTime int64, accountMetadata map[string]*commonpb.MetadataMap) *servicepb.Request {
	payload := &servicepb.CreateTransactionPayload{Postings: postings, Reference: fmt.Sprintf("%d:%s", height, txID), Metadata: metadata, Timestamp: &commonpb.Timestamp{Data: uint64(blockTime) * 1_000_000}, AccountMetadata: accountMetadata}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: moduleConfig.Ledger, Action: action}}}
}

func baseMetadata(height uint64, source tx, b *block) map[string]*commonpb.MetadataValue {
	return map[string]*commonpb.MetadataValue{"block_height": {Type: &commonpb.MetadataValue_StringValue{StringValue: strconv.FormatUint(height, 10)}}, "block_hash": {Type: &commonpb.MetadataValue_StringValue{StringValue: b.Hash}}, "tx_hash": {Type: &commonpb.MetadataValue_StringValue{StringValue: source.TxID}}}
}

func mapRegular(height uint64, source tx, b *block) ([]*commonpb.Posting, map[string]*commonpb.MetadataValue, []feeEntry, error) {
	metadata := baseMetadata(height, source, b)
	var postings []*commonpb.Posting
	staging := "staging:" + sanitize(source.TxID)
	var inputs, outputs uint64
	for _, input := range source.Vin {
		if input.Prevout == nil {
			continue
		}
		address := resolveAddress(input.Prevout.Script)
		if address == "" {
			continue
		}
		amount, err := satoshis(input.Prevout.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		inputs += amount
		postings = append(postings, posting(moduleConfig.AddressPref+":"+sanitize(address), staging, amount))
	}
	for _, output := range source.Vout {
		address := resolveAddress(output.Script)
		if address == "" {
			continue
		}
		amount, err := satoshis(output.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		outputs += amount
		postings = append(postings, posting(staging, moduleConfig.AddressPref+":"+sanitize(address), amount))
	}
	var fees []feeEntry
	if inputs > outputs {
		fee := inputs - outputs
		account := fmt.Sprintf("fees:%d:%s", height, sanitize(source.TxID))
		postings = append(postings, posting(staging, account, fee))
		fees = append(fees, feeEntry{account: account, amount: fee})
		metadata["total_fees"] = metadataUint(fee)
	} else if outputs > inputs {
		deficit := outputs - inputs
		imprecise := "imprecise_mapping:" + sanitize(source.TxID)
		postings = append([]*commonpb.Posting{posting("world", imprecise, deficit), posting(imprecise, staging, deficit)}, postings...)
		metadata["imprecise_mapping"] = metadataBool(true)
	}
	if len(postings) == 0 {
		postings = append(postings, posting("world", "imprecise_mapping:"+sanitize(source.TxID), 1), posting("imprecise_mapping:"+sanitize(source.TxID), "burn", 1))
		metadata["zero_flow"] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}}
		metadata["imprecise_mapping"] = metadataBool(true)
	}
	return postings, metadata, fees, nil
}

func mapCoinbase(height uint64, source tx, b *block, fees []feeEntry) ([]*commonpb.Posting, map[string]*commonpb.MetadataValue, map[string]*commonpb.MetadataMap, error) {
	metadata := baseMetadata(height, source, b)
	metadata["coinbase"] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}}
	var address string
	selectedOutput := -1
	for i, output := range source.Vout {
		if resolved := resolveAddress(output.Script); resolved != "" {
			address = resolved
			selectedOutput = i
			break
		}
	}
	var destination string
	if address != "" {
		destination = moduleConfig.AddressPref + ":" + sanitize(address)
	}
	postings := make([]*commonpb.Posting, 0, len(source.Vout)+len(fees)+2)
	if destination == "" {
		if subsidy := blockSubsidy(height); subsidy > 0 {
			postings = append(postings, posting("world", "burn", subsidy))
		}
		for _, fee := range fees {
			postings = append(postings, posting(fee.account, "burn", fee.amount))
		}
	} else {
		mining := "mining:" + sanitize(address)
		subsidy := blockSubsidy(height)
		if subsidy > 0 {
			postings = append(postings, posting("world", mining, subsidy), posting(mining, destination, subsidy))
		}
		for i, output := range source.Vout {
			if i == selectedOutput {
				continue
			}
			resolved := resolveAddress(output.Script)
			if resolved == "" {
				continue
			}
			amount, err := satoshis(output.Value)
			if err != nil {
				return nil, nil, nil, err
			}
			postings = append(postings, posting("world", moduleConfig.AddressPref+":"+sanitize(resolved), amount))
		}
		for _, fee := range fees {
			postings = append(postings, posting(fee.account, destination, fee.amount))
		}
	}
	metadata["subsidy"] = metadataUint(blockSubsidy(height))
	var totalFees uint64
	for _, fee := range fees {
		totalFees += fee.amount
	}
	metadata["total_fees"] = metadataUint(totalFees)
	accountMetadata := make(map[string]*commonpb.MetadataMap)
	for _, p := range postings {
		if p.GetSource() == "world" || p.GetDestination() == "" || strings.HasPrefix(p.GetDestination(), "fees:") {
			continue
		}
		accountMetadata[p.GetDestination()] = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{"miner": metadataBool(true)}}
	}
	return postings, metadata, accountMetadata, nil
}

func resolveAddress(value script) string {
	if value.Address != "" {
		return value.Address
	}
	return p2pkToAddress(value.Hex)
}

func p2pkToAddress(scriptHex string) string {
	script, err := hex.DecodeString(scriptHex)
	if err != nil || len(script) < 2 || script[len(script)-1] != 0xac {
		return ""
	}
	pushLen := int(script[0])
	pubkey := script[1 : len(script)-1]
	if len(pubkey) != pushLen || (len(pubkey) != 33 && len(pubkey) != 65) {
		return ""
	}
	if len(pubkey) == 65 && pubkey[0] == 0x04 {
		compressed := make([]byte, 33)
		if pubkey[64]%2 == 0 {
			compressed[0] = 0x02
		} else {
			compressed[0] = 0x03
		}
		copy(compressed[1:], pubkey[1:33])
		pubkey = compressed
	}
	if len(pubkey) != 33 || (pubkey[0] != 0x02 && pubkey[0] != 0x03) {
		return ""
	}
	h := sha256.Sum256(pubkey)
	r := ripemd160.New()
	_, _ = r.Write(h[:])
	payload := append([]byte{0}, r.Sum(nil)...)
	first := sha256.Sum256(payload)
	second := sha256.Sum256(first[:])
	return base58Encode(append(payload, second[:4]...))
}

func base58Encode(input []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	leading := 0
	for leading < len(input) && input[leading] == 0 {
		leading++
	}
	buf := make([]byte, len(input)*138/100+1)
	length := 0
	for _, b := range input {
		carry := int(b)
		for i := 0; i < length || carry != 0; i++ {
			if i < length {
				carry += 256 * int(buf[i])
			}
			buf[i] = byte(carry % 58)
			carry /= 58
			if i >= length {
				length = i + 1
			}
		}
	}
	result := make([]byte, leading+length)
	for i := 0; i < leading; i++ {
		result[i] = '1'
	}
	for i := 0; i < length; i++ {
		result[leading+i] = alphabet[buf[length-1-i]]
	}
	return string(result)
}

func metadataBool(value bool) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: value}}
}

func metadataUint(value uint64) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_UintValue{UintValue: value}}
}

func blockSubsidy(height uint64) uint64 {
	halvings := height / 210000
	if halvings >= 64 {
		return 0
	}
	return 5000000000 >> halvings
}

func posting(source, destination string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{Source: source, Destination: destination, Amount: commonpb.NewUint256FromUint64(amount), Asset: "BTC/8"}
}
func satoshis(value json.Number) (uint64, error) {
	parts := strings.Split(value.String(), ".")
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	for len(frac) < 8 {
		frac += "0"
	}
	if len(frac) > 8 {
		frac = frac[:8]
	}
	minor, err := strconv.ParseUint(frac, 10, 64)
	return whole*100000000 + minor, err
}
func sanitize(value string) string {
	var out strings.Builder
	for _, c := range value {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			out.WriteRune(c)
		} else {
			out.WriteByte('_')
		}
	}
	return out.String()
}

func rpc(method string, params ...any) ([]byte, error) {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "1.0", "id": 1, "method": method, "params": params})
	auth := base64.StdEncoding.EncodeToString([]byte(moduleConfig.RPCUser + ":" + moduleConfig.RPCPassword))
	req := fmt.Sprintf("POST / HTTP/1.1\r\nHost: %s\r\nAuthorization: Basic %s\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s", moduleConfig.RPCHost, auth, len(body), body)
	host := []byte(moduleConfig.RPCHost)
	request := []byte(req)
	handle := netRequest(uint32(uintptr(unsafe.Pointer(&host[0]))), uint32(len(host)), moduleConfig.RPCPort, uint32(uintptr(unsafe.Pointer(&request[0]))), uint32(len(request)))
	if handle == ^uint32(0) {
		return nil, fmt.Errorf("network request failed")
	}
	defer netClose(handle)
	for {
		status := netPoll(handle)
		if status == 1 {
			break
		}
		if status == ^uint32(0) {
			return nil, fmt.Errorf("network request failed")
		}
		time.Sleep(time.Millisecond)
	}
	responseSize := netResponseSize(handle)
	if responseSize == ^uint32(0) {
		return nil, fmt.Errorf("network response size failed")
	}
	buffer := make([]byte, responseSize)
	if len(buffer) == 0 {
		return nil, fmt.Errorf("empty network response")
	}
	responseLen := netResponseRead(handle, uint32(uintptr(unsafe.Pointer(&buffer[0]))), uint32(len(buffer)))
	if responseLen == ^uint32(0) {
		return nil, fmt.Errorf("network response read failed")
	}
	response := buffer[:responseLen]
	parts := bytes.SplitN(response, []byte("\r\n\r\n"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid HTTP response")
	}
	var envelope rpcEnvelope
	if err := json.Unmarshal(parts[1], &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	return envelope.Result, nil
}
func dial(host string, port uint32) (uint32, error) {
	b := []byte(host)
	id := netConnect(uint32(uintptr(unsafe.Pointer(&b[0]))), uint32(len(b)), port)
	if id == ^uint32(0) {
		return 0, fmt.Errorf("network connect failed")
	}
	return id, nil
}
func writeAll(handle uint32, data []byte) error {
	for len(data) > 0 {
		count := netWrite(handle, uint32(uintptr(unsafe.Pointer(&data[0]))), uint32(len(data)))
		if count == ^uint32(0) || count == 0 {
			return fmt.Errorf("network write failed")
		}
		data = data[count:]
	}
	return nil
}
func readAll(handle uint32) ([]byte, error) {
	var result []byte
	buffer := make([]byte, 32<<10)
	for {
		count := netRead(handle, uint32(uintptr(unsafe.Pointer(&buffer[0]))), uint32(len(buffer)))
		if count == ^uint32(0) {
			return nil, fmt.Errorf("network read failed")
		}
		if count == 0 {
			return result, nil
		}
		result = append(result, buffer[:count]...)
	}
}
func logf(level uint32, format string, args ...any) {
	text := []byte(fmt.Sprintf(format, args...))
	if len(text) == 0 {
		ledgerLog(level, 0, 0)
		return
	}
	ledgerLog(level, uint32(uintptr(unsafe.Pointer(&text[0]))), uint32(len(text)))
}
