package integration

import (
	"context"

	"github.com/formancehq/payments/internal/app/messages"

	"github.com/formancehq/go-libs/publish"

	"github.com/formancehq/payments/internal/app/storage"

	"github.com/google/uuid"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/task"
	"github.com/pkg/errors"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyInstalled = errors.New("already installed")
	ErrNotInstalled     = errors.New("not installed")
	ErrNotEnabled       = errors.New("not enabled")
	ErrAlreadyRunning   = errors.New("already running")
)

type ConnectorManager[Config models.ConnectorConfigObject] struct {
	logger           logging.Logger
	loader           Loader[Config]
	connector        Connector
	store            Repository
	schedulerFactory TaskSchedulerFactory
	scheduler        *task.DefaultTaskScheduler
	publisher        publish.Publisher
}

func (l *ConnectorManager[ConnectorConfig]) Enable(ctx context.Context) error {
	l.logger.Info("Enabling connector")

	err := l.store.Enable(ctx, l.loader.Name())
	if err != nil {
		return err
	}

	return nil
}

func (l *ConnectorManager[ConnectorConfig]) ReadConfig(ctx context.Context,
) (*ConnectorConfig, error) {
	var config ConnectorConfig

	connector, err := l.store.GetConnector(ctx, l.loader.Name())
	if err != nil {
		return &config, err
	}

	err = connector.ParseConfig(&config)
	if err != nil {
		return &config, err
	}

	config = l.loader.ApplyDefaults(config)

	return &config, nil
}

func (l *ConnectorManager[ConnectorConfig]) load(config ConnectorConfig) {
	l.connector = l.loader.Load(l.logger, config)
	l.scheduler = l.schedulerFactory.Make(l.connector, l.loader.AllowTasks())
}

func (l *ConnectorManager[ConnectorConfig]) Install(ctx context.Context, config ConnectorConfig) error {
	l.logger.WithFields(map[string]interface{}{
		"config": config,
	}).Infof("Install connector %s", l.loader.Name())

	isInstalled, err := l.store.IsInstalled(ctx, l.loader.Name())
	if err != nil {
		l.logger.Errorf("Error checking if connector is installed: %s", err)

		return err
	}

	if isInstalled {
		l.logger.Errorf("Connector already installed")

		return ErrAlreadyInstalled
	}

	config = l.loader.ApplyDefaults(config)

	if err = config.Validate(); err != nil {
		return err
	}

	l.load(config)

	cfg, err := config.Marshal()
	if err != nil {
		return err
	}

	err = l.store.Install(ctx, l.loader.Name(), cfg)
	if err != nil {
		return err
	}

	err = l.connector.Install(task.NewConnectorContext(context.Background(), l.scheduler))
	if err != nil {
		l.logger.Errorf("Error starting connector: %s", err)

		return err
	}

	l.logger.Infof("Connector installed")

	return nil
}

func (l *ConnectorManager[ConnectorConfig]) Uninstall(ctx context.Context) error {
	l.logger.Infof("Uninstalling connector")

	isInstalled, err := l.IsInstalled(ctx)
	if err != nil {
		l.logger.Errorf("Error checking if connector is installed: %s", err)

		return err
	}

	if !isInstalled {
		l.logger.Errorf("Connector not installed")

		return ErrNotInstalled
	}

	err = l.scheduler.Shutdown(ctx)
	if err != nil {
		return err
	}

	err = l.connector.Uninstall(ctx)
	if err != nil {
		return err
	}

	err = l.store.Uninstall(ctx, l.loader.Name())
	if err != nil {
		return err
	}

	l.logger.Info("Connector uninstalled")

	return nil
}

func (l *ConnectorManager[ConnectorConfig]) Restore(ctx context.Context) error {
	l.logger.Info("Restoring state")

	installed, err := l.IsInstalled(ctx)
	if err != nil {
		return err
	}

	if !installed {
		l.logger.Info("Not installed, skip")

		return ErrNotInstalled
	}

	enabled, err := l.IsEnabled(ctx)
	if err != nil {
		return err
	}

	if !enabled {
		l.logger.Info("Not enabled, skip")

		return ErrNotEnabled
	}

	if l.connector != nil {
		return ErrAlreadyRunning
	}

	config, err := l.ReadConfig(ctx)
	if err != nil {
		return err
	}

	l.load(*config)

	err = l.scheduler.Restore(ctx)
	if err != nil {
		l.logger.Errorf("Unable to restore scheduler: %s", err)

		return err
	}

	l.logger.Info("State restored")

	return nil
}

func (l *ConnectorManager[ConnectorConfig]) Disable(ctx context.Context) error {
	l.logger.Info("Disabling connector")

	return l.store.Disable(ctx, l.loader.Name())
}

func (l *ConnectorManager[ConnectorConfig]) IsEnabled(ctx context.Context) (bool, error) {
	return l.store.IsEnabled(ctx, l.loader.Name())
}

func (l *ConnectorManager[ConnectorConfig]) FindAll(ctx context.Context) ([]models.Connector, error) {
	return l.store.FindAll(ctx)
}

func (l *ConnectorManager[ConnectorConfig]) IsInstalled(ctx context.Context) (bool, error) {
	return l.store.IsInstalled(ctx, l.loader.Name())
}

func (l *ConnectorManager[ConnectorConfig]) ListTasksStates(ctx context.Context, pagination storage.Paginator,
) ([]models.Task, storage.PaginationDetails, error) {
	return l.scheduler.ListTasks(ctx, pagination)
}

func (l *ConnectorManager[Config]) ReadTaskState(ctx context.Context, taskID uuid.UUID) (*models.Task, error) {
	return l.scheduler.ReadTask(ctx, taskID)
}

func (l *ConnectorManager[ConnectorConfig]) Reset(ctx context.Context) error {
	config, err := l.ReadConfig(ctx)
	if err != nil {
		return err
	}

	err = l.Uninstall(ctx)
	if err != nil {
		return err
	}

	err = l.Install(ctx, *config)
	if err != nil {
		return err
	}

	err = l.publisher.Publish(ctx, messages.TopicPayments,
		messages.NewEventResetConnector(l.loader.Name()))
	if err != nil {
		l.logger.Errorf("Publishing message: %w", err)
	}

	return nil
}

func NewConnectorManager[ConnectorConfig models.ConnectorConfigObject](
	logger logging.Logger,
	store Repository,
	loader Loader[ConnectorConfig],
	schedulerFactory TaskSchedulerFactory,
	publisher publish.Publisher,
) *ConnectorManager[ConnectorConfig] {
	return &ConnectorManager[ConnectorConfig]{
		logger: logger.WithFields(map[string]interface{}{
			"component": "connector-manager",
			"provider":  loader.Name(),
		}),
		store:            store,
		loader:           loader,
		schedulerFactory: schedulerFactory,
		publisher:        publisher,
	}
}
