package workflow

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

const TaskQueueName = "queue"

var (
	ErrOccurrenceNotFound = errors.New("occurence not found")
)

type Manager struct {
	db             *bun.DB
	temporalClient client.Client
}

func (m *Manager) Create(ctx context.Context, config Config) (*Workflow, error) {

	if err := config.Validate(); err != nil {
		return nil, err
	}

	workflow := New(config)

	if _, err := m.db.
		NewInsert().
		Model(&workflow).
		Exec(ctx); err != nil {
		return nil, err
	}

	return &workflow, nil
}

func (m *Manager) RunWorkflow(ctx context.Context, id string, variables map[string]string) (Occurrence, error) {

	workflow := Workflow{}
	if err := m.db.NewSelect().
		Where("id = ?", id).
		Model(&workflow).
		Scan(ctx); err != nil {
		return Occurrence{}, err
	}

	workflowRun := newOccurrence(id)

	if _, err := m.db.
		NewInsert().
		Model(&workflowRun).
		Exec(ctx); err != nil {
		return Occurrence{}, err
	}

	_, err := m.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowRun.ID,
		TaskQueue: TaskQueueName,
	}, Run, Input{
		Config:    workflow.Config,
		Variables: variables,
	})
	if err != nil {
		return Occurrence{}, err
	}

	return workflowRun, nil
}

func (m *Manager) Wait(ctx context.Context, workflowID, runID string) error {
	if err := m.temporalClient.
		GetWorkflow(ctx, workflowID, runID).
		Get(ctx, nil); err != nil {
		if errors.Is(err, &serviceerror.NotFound{}) {
			return ErrOccurrenceNotFound
		}
		return errors.Unwrap(err)
	}
	return nil
}

func (m *Manager) ListWorkflows(ctx context.Context) ([]Workflow, error) {
	workflows := make([]Workflow, 0)
	if err := m.db.NewSelect().
		Model(&workflows).
		Scan(ctx); err != nil {
		return nil, err
	}
	return workflows, nil
}

func (m *Manager) ReadWorkflow(ctx context.Context, id string) (Workflow, error) {
	var workflow Workflow
	if err := m.db.NewSelect().
		Model(&workflow).
		Where("id = ?", id).
		Scan(ctx); err != nil {
		return Workflow{}, err
	}
	return workflow, nil
}

func (m *Manager) PostEvent(ctx context.Context, workflowRunID string, event event) error {
	workflowRun := Occurrence{}
	if err := m.db.NewSelect().
		Model(&workflowRun).
		Where("id = ?", workflowRunID).
		Scan(ctx); err != nil {
		return errors.Wrap(err, "retrieving workflow")
	}

	err := m.temporalClient.SignalWorkflow(ctx, workflowRun.ID, "", waitEventSignalName, event)
	if err != nil {
		return errors.Wrap(err, "sending signal to server")
	}

	return nil
}

func (m *Manager) AbortRun(ctx context.Context, workflowRunID string) error {
	workflowRun := Occurrence{}
	if err := m.db.NewSelect().
		Model(&workflowRun).
		Where("id", workflowRunID).
		Scan(ctx); err != nil {
		return errors.Wrap(err, "retrieving workflow execution")
	}

	return m.temporalClient.CancelWorkflow(ctx, workflowRunID, "")
}

func (m *Manager) ListExecutions(ctx context.Context, workflowID string) ([]Occurrence, error) {
	workflowRuns := make([]Occurrence, 0)
	if err := m.db.NewSelect().
		Model(&workflowRuns).
		Where("workflow_id = ?", workflowID).
		Scan(ctx); err != nil {
		return nil, errors.Wrap(err, "retrieving workflow")
	}
	return workflowRuns, nil
}

func (m *Manager) ReadWorkflowRunHistory(ctx context.Context, workflowRunID string) ([]*history.HistoryEvent, error) {
	historyIterator := m.temporalClient.GetWorkflowHistory(ctx, workflowRunID, "",
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	events := make([]*history.HistoryEvent, 0)
	for historyIterator.HasNext() {
		event, err := historyIterator.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (m *Manager) GetOccurrence(ctx context.Context, workflowID string, occurrenceID string) (*Occurrence, error) {
	occurrence := Occurrence{}
	err := m.db.NewSelect().
		Model(&occurrence).
		Relation("Statuses").
		Where("workflow_id = ?", workflowID).
		Where("id = ?", occurrenceID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrOccurrenceNotFound
		}
		return nil, err
	}
	return &occurrence, nil
}

func NewManager(db *bun.DB, temporalClient client.Client) *Manager {
	return &Manager{
		db:             db,
		temporalClient: temporalClient,
	}
}
