package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/pkg/errors"

	"github.com/formancehq/payments/internal/app/models"
)

func (s *Storage) UpdateTaskStatus(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor, status models.TaskStatus, taskError string) error {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return e("failed to get connector", err)
	}

	_, err = s.db.NewUpdate().Model(&models.Task{}).
		Set("status = ?", status).
		Set("error = ?", taskError).
		Where("descriptor::TEXT = ?::TEXT", descriptor.ToMessage()).
		Where("connector_id = ?", connector.ID).
		Exec(ctx)
	if err != nil {
		return e("failed to update task", err)
	}

	return nil
}

func (s *Storage) UpdateTaskState(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor, state json.RawMessage) error {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return e("failed to get connector", err)
	}

	_, err = s.db.NewUpdate().Model(&models.Task{}).
		Set("state = ?", state).
		Where("descriptor::TEXT = ?::TEXT", descriptor.ToMessage()).
		Where("connector_id = ?", connector.ID).
		Exec(ctx)
	if err != nil {
		return e("failed to update task", err)
	}

	return nil
}

func (s *Storage) FindAndUpsertTask(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor, status models.TaskStatus, taskErr string) (*models.Task, error) {
	_, err := s.GetTaskByDescriptor(ctx, provider, descriptor)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, e("failed to get task", err)
	}

	if err == nil {
		err = s.UpdateTaskStatus(ctx, provider, descriptor, status, taskErr)
		if err != nil {
			return nil, e("failed to update task", err)
		}
	} else {
		err = s.CreateTask(ctx, provider, descriptor, status)
		if err != nil {
			return nil, e("failed to upsert task", err)
		}
	}

	return s.GetTaskByDescriptor(ctx, provider, descriptor)
}

func (s *Storage) CreateTask(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor, status models.TaskStatus) error {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return e("failed to get connector", err)
	}

	_, err = s.db.NewInsert().Model(&models.Task{
		ConnectorID: connector.ID,
		Descriptor:  descriptor.ToMessage(),
		Status:      status,
	}).Exec(ctx)
	if err != nil {
		return e("failed to create task", err)
	}

	return nil
}

func (s *Storage) ListTasksByStatus(ctx context.Context, provider models.ConnectorProvider, status models.TaskStatus) ([]models.Task, error) {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return nil, e("failed to get connector", err)
	}

	var tasks []models.Task

	err = s.db.NewSelect().Model(&tasks).
		Where("connector_id = ?", connector.ID).
		Where("status = ?", status).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get tasks", err)
	}

	return tasks, nil
}

func (s *Storage) ListTasks(ctx context.Context, provider models.ConnectorProvider, pagination Paginator) ([]models.Task, PaginationDetails, error) {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return nil, PaginationDetails{}, e("failed to get connector", err)
	}

	var tasks []models.Task

	query := s.db.NewSelect().Model(&tasks).
		Where("connector_id = ?", connector.ID)

	query = pagination.apply(query, "task.created_at")

	err = query.Scan(ctx)
	if err != nil {
		return nil, PaginationDetails{}, e("failed to get tasks", err)
	}

	var (
		hasMore                       = len(tasks) > pagination.pageSize
		firstReference, lastReference string
	)

	if hasMore {
		tasks = tasks[:pagination.pageSize]
	}

	if len(tasks) > 0 {
		firstReference = tasks[0].CreatedAt.Format(time.RFC3339Nano)
		lastReference = tasks[len(tasks)-1].CreatedAt.Format(time.RFC3339Nano)
	}

	paginationDetails, err := pagination.paginationDetails(hasMore, firstReference, lastReference)
	if err != nil {
		return nil, PaginationDetails{}, fmt.Errorf("failed to get pagination details: %w", err)
	}

	return tasks, paginationDetails, nil
}

func (s *Storage) ReadOldestPendingTask(ctx context.Context, provider models.ConnectorProvider) (*models.Task, error) {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return nil, e("failed to get connector", err)
	}

	var task models.Task

	err = s.db.NewSelect().Model(&task).
		Where("connector_id = ?", connector.ID).
		Where("status = ?", models.TaskStatusPending).
		Order("created_at ASC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get task", err)
	}

	return &task, nil
}

func (s *Storage) GetTask(ctx context.Context, id uuid.UUID) (*models.Task, error) {
	var task models.Task

	err := s.db.NewSelect().Model(&task).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get task", err)
	}

	return &task, nil
}

func (s *Storage) GetTaskByDescriptor(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor) (*models.Task, error) {
	connector, err := s.GetConnector(ctx, provider)
	if err != nil {
		return nil, e("failed to get connector", err)
	}

	var task models.Task

	err = s.db.NewSelect().Model(&task).
		Where("connector_id = ?", connector.ID).
		Where("descriptor::TEXT = ?::TEXT", descriptor.ToMessage()).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get task", err)
	}

	return &task, nil
}
