package service

import (
	"encoding/json"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// CommandType represents the type of command
type CommandType string

const (
	// CommandTypeInsertLogs is the command type for inserting ledger logs
	CommandTypeInsertLogs CommandType = "insert_logs"
	// CommandTypeSetPublicAddr is the command type for setting a node's public address
	CommandTypeSetPublicAddr CommandType = "set_public_addr"
)

// Command represents a command to be executed in the FSM
type Command struct {
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

// InsertLogsCommand represents the data for an insert logs command
type InsertLogsCommand struct {
	Logs []ledger.Log `json:"logs"`
}

// NewInsertLogsCommand creates a new InsertLogsCommand
func NewInsertLogsCommand(logs []ledger.Log) (*Command, error) {
	data, err := json.Marshal(InsertLogsCommand{Logs: logs})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeInsertLogs,
		Data: data,
	}, nil
}

// SetPublicAddrCommand represents the data for a set public address command
type SetPublicAddrCommand struct {
	NodeID    string `json:"nodeId"`
	PublicAddr string `json:"publicAddr"`
}

// NewSetPublicAddrCommand creates a new SetPublicAddrCommand
func NewSetPublicAddrCommand(nodeID, publicAddr string) (*Command, error) {
	data, err := json.Marshal(SetPublicAddrCommand{
		NodeID:    nodeID,
		PublicAddr: publicAddr,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeSetPublicAddr,
		Data: data,
	}, nil
}

