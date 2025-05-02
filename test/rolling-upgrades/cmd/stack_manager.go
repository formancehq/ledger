package cmd

import (
	"context"
	"fmt"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optdestroy"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path/filepath"
)

type Logger interface {
	Log(fmt string, args ...any)
}
type LoggerFn func(fmt string, args ...any)

func (fn LoggerFn) Log(fmt string, args ...any) {
	fn(fmt, args...)
}

type Stack struct {
	logger              Logger
	progressStream      io.Writer
	errorProgressStream io.Writer
}

func (s *Stack) updateConfigFile(configFileName string, cfg any) error {
	tmpConfigFile, err := os.Create(configFileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpConfigFile.Close()
	}()

	return yaml.NewEncoder(tmpConfigFile).Encode(struct {
		Config any `yaml:"config"`
	}{
		Config: cfg,
	})
}

func (s *Stack) Deploy(ctx context.Context, stack auto.Stack, config any, options ...optup.Option) (auto.UpResult, error) {

	projectSettings, err := stack.Workspace().ProjectSettings(ctx)
	if err != nil {
		return auto.UpResult{}, fmt.Errorf("failed to get project settings: %w", err)
	}

	s.logger.Log("Deploying stack %s/%s...\r\n", projectSettings.Name, stack.Name())

	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return auto.UpResult{}, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	tmpConfigFileName := filepath.Join(tmpDir, "config.yaml")
	tmpConfigFile, err := os.Create(tmpConfigFileName)
	if err != nil {
		return auto.UpResult{}, err
	}
	defer func() {
		_ = tmpConfigFile.Close()
	}()

	if err := s.updateConfigFile(tmpConfigFileName, config); err != nil {
		return auto.UpResult{}, fmt.Errorf("failed to update config file: %w", err)
	}

	options = append(options,
		optup.ConfigFile(tmpConfigFileName),
		optup.ProgressStreams(s.progressStream),
		optup.ErrorProgressStreams(s.errorProgressStream),
		optup.Color("always"),
	)

	defer func() {
		// Just cancel in case of cancellation
		_ = stack.Cancel(ctx)
	}()

	result, err := stack.Up(ctx, options...)
	if err != nil {
		return auto.UpResult{}, fmt.Errorf("failed to up stack: %w", err)
	}

	s.logger.Log("Stack %s/%s deployed...\r\n", projectSettings.Name, stack.Name())

	return result, nil
}

func (s *Stack) Destroy(ctx context.Context, stack auto.Stack, options ...optdestroy.Option) error {

	projectSettings, err := stack.Workspace().ProjectSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to get project settings: %w", err)
	}

	s.logger.Log("Destroying stack %s/%s...\r\n", projectSettings.Name, stack.Name())

	options = append(options,
		optdestroy.ProgressStreams(s.progressStream),
		optdestroy.ErrorProgressStreams(s.errorProgressStream),
		optdestroy.Color("always"),
	)

	_, err = stack.Destroy(ctx, options...)
	if err != nil {
		return fmt.Errorf("failed to destroy stack: %w", err)
	}

	s.logger.Log("Stack %s/%s destroyed.\r\n", projectSettings.Name, stack.Name())

	return nil
}

func NewStackManager(options ...DeploymentOption) *Stack {
	ret := &Stack{}
	for _, option := range append(defaultOptions, options...) {
		option(ret)
	}
	return ret
}

type DeploymentOption func(*Stack)

func WithProgressStream(writer io.Writer) DeploymentOption {
	return func(s *Stack) {
		s.progressStream = writer
	}
}

func WithErrorProgressStream(writer io.Writer) DeploymentOption {
	return func(s *Stack) {
		s.errorProgressStream = writer
	}
}

func WithLogger(logger Logger) DeploymentOption {
	return func(s *Stack) {
		s.logger = logger
	}
}

var defaultOptions = []DeploymentOption{
	WithProgressStream(io.Discard),
	WithErrorProgressStream(io.Discard),
	WithLogger(LoggerFn(func(fmt string, args ...any) {})),
}
