package storage

import (
	"fmt"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type externalDatabaseComponent struct {
	pulumi.ResourceState

	Endpoint pulumix.Input[string]
	Username pulumix.Input[string]
	Password pulumix.Input[string]
	Port     pulumix.Input[int]
	Options  pulumix.Input[map[string]string]
	Database pulumix.Input[string]
}

func (cmp *externalDatabaseComponent) GetEndpoint() pulumix.Input[string] {
	return cmp.Endpoint
}

func (cmp *externalDatabaseComponent) GetUsername() pulumix.Input[string] {
	return cmp.Username
}

func (cmp *externalDatabaseComponent) GetPassword() pulumix.Input[string] {
	return cmp.Password
}

func (cmp *externalDatabaseComponent) GetPort() pulumix.Input[int] {
	return cmp.Port
}

func (cmp *externalDatabaseComponent) GetOptions() pulumix.Input[map[string]string] {
	return cmp.Options
}

func (cmp *externalDatabaseComponent) GetDatabase() pulumix.Input[string] {
	return cmp.Database
}

type ExternalDatabaseComponentArgs struct {
	Endpoint pulumix.Input[string]
	Username pulumix.Input[string]
	Password pulumix.Input[string]
	Port     pulumix.Input[int]
	Options  pulumix.Input[map[string]string]
	Database pulumix.Input[string]
}

func newExternalDatabaseComponent(ctx *pulumi.Context, name string, args ExternalDatabaseComponentArgs, opts ...pulumi.ResourceOption) (*externalDatabaseComponent, error) {
	cmp := &externalDatabaseComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:ExternalPostgres", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.Endpoint = args.Endpoint
	cmp.Username = args.Username
	cmp.Password = args.Password
	cmp.Port = args.Port
	cmp.Options = args.Options
	cmp.Database = args.Database

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}

var _ databaseComponent = (*externalDatabaseComponent)(nil)
