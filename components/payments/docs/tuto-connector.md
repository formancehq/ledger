# Tutorial connector

_referenced in `/pkg/bridge/connectors/dummypay`_

We are going to create a fake connector which read a directory.
In this directory, a fake bank service will create files.
Each files contain a payin or a payout as a json object.

First, to create a connector, we need a loader.

## The loader object

```go
type Loader[ConnectorConfig payments.ConnectorConfigObject] interface {
	// Name has to return the name of the connector. It must be constant and unique
	Name() string
	// Load is in charge of loading the connector
	// It takes a logger and a ConnectorConfig object.
	// At this point, the config must have been validated
	Load(logger sharedlogging.Logger, config ConnectorConfig) Connector
	// ApplyDefaults is used to fill default values of the provided configuration object.
	ApplyDefaults(t ConnectorConfig) ConnectorConfig
	// AllowTasks define how many task the connector can run
	// If too many tasks are scheduled by the connector,
	// those will be set to pending state and restarted later when some other tasks will be terminated
	AllowTasks() int
}
```

A connector has a name.

This name is provided by the loader by the method Name().
Also, each connector define a config object using generics which has to implement interface payments.ConnectorConfigObject.
This interface only have a method Validate() error which is used by the code to validate an external config is valid before load the connector with it.
Since, some properties of the config may have some optional, the loader is also in charge of configuring default values on it.
This is done using the method ```ApplyDefaults(Config)```.

The framework provide the capabilities to run tasks.
So each connector can start any number of tasks.
Those tasks will be scheduled by the framework. For example, if the service is restarted, the tasks will be restarted at reboot.
The number of tasks a connector can schedule is defined by the method AllowTasks().

To implement Loader interface, you can create your own struct implementing required methods, or you can use some utilities provided by the framework.
Let`s create a basic loader.

```go
type (
	Config struct {}
	TaskDescriptor struct {}
)

func (cfg Config) Validate() error {
	return nil
}

var Loader = integration.NewLoaderBuilder[Config]("example").Build()
```

Here, we built our loader.
The name of the connector is "example".
For now, the ```Config``` and ```TaskDescriptor``` are just empty structs, we will change it later.
Also, we didn't define any logic on our connector.

It's time to plug our connector on the core.
Edit the file cmd/root.go and go at the end of the method HTTPModule(), you should find a code like this :
```go
    ...
    cdi.ConnectorModule[stripe.Config, stripe.TaskDescriptor](
        viper.GetBool(authBearerUseScopesFlag),
        stripe.NewLoader(),
    ),
    ...
```

You can add your connector bellow that :
```go
    ...
    cdi.ConnectorModule[example.Config, example.TaskDescriptor](
        viper.GetBool(authBearerUseScopesFlag),
        example.Loader,
    ),
    ...
```

Now you, can run the service and you should see something like this :
```bash
payments-payments-1  | time="2022-07-01T09:12:21Z" level=info msg="Restoring state" component=connector-manager provider=example
payments-payments-1  | time="2022-07-01T09:12:21Z" level=info msg="Not installed, skip" component=connector-manager provider=example
```

This indicates your connector is properly integrated.
You can install it like this :
```bash
curl http://localhost:8080/connectors/example -X POST
```

The service will display something like this :
```bash
payments-payments-1  | time="2022-07-01T10:04:53Z" level=info msg="Install connector example" component=connector-manager config="{}" provider=example
payments-payments-1  | time="2022-07-01T10:04:53Z" level=info msg="Connector installed" component=connector-manager provider=example
```

Your connector was installed!
It makes nothing but it is installed.

Let's uninstall it before continue :
```bash
curl http://localhost:8080/connectors/example -X DELETE
```

You should see something like this :
```bash
payments-payments-1  | time="2022-07-01T10:06:16Z" level=info msg="Uninstalling connector" component=connector-manager provider=example
payments-payments-1  | time="2022-07-01T10:06:16Z" level=info msg="Stopping scheduler..." component=scheduler provider=example
payments-payments-1  | time="2022-07-01T10:06:16Z" level=info msg="Connector uninstalled" component=connector-manager provider=example
```

It's to time to add a bit of logic to our connector.

As you may have noticed, the ```Loader``` has method named ```Load``` :
```go
...
Load(logger sharedlogging.Logger, config ConnectorConfig) Connector[TaskDescriptor]
...
```

The Load function take a logger provided by the framework and a config, probably provided by the api endpoint.
It has to return a Connector object. Here the interface :
```go
// Connector provide entry point to a payment provider
type Connector interface {
	// Install is used to start the connector. The implementation if in charge of scheduling all required resources.
	Install(ctx task.ConnectorContext) error
	// Uninstall is used to uninstall the connector. It has to close all related resources opened by the connector.
	Uninstall(ctx context.Context) error
	// Resolve is used to recover state of a failed or restarted task
	Resolve(descriptor TaskDescriptor) task.Task
}
```

When you made ```curl http://localhost:8080/connectors/example -X POST```, the framework called the ```Install()``` method.
When you made ```curl http://localhost:8080/connectors/example -X DELETE```, the framework called the ```Uninstall(ctx context.Context) error``` method.

It's time to add some logic. We have to modify our loader but before let's add some property to our config :
```go
type (
	Config struct {
		Directory string
	}
	...
)

func (cfg Config) Validate() error {
	if cfg.Directory == "" {
		return errors.New("missing directory to watch")
	}
	return nil
}
```

Here we defined only one property to our connector, "Directory", which indicates the directory when json files will be pushed.
Now, modify our loader :
```go
var Loader = integration.NewLoaderBuilder[Config]("example").
	WithLoad(func(logger sharedlogging.Logger, config Config) integration.Connector {
		return integration.NewConnectorBuilder().
			WithInstall(func(ctx task.ConnectorContext) error {
				return errors.New("not implemented")
			}).
			Build()
	}).
	Build()
```

Here we create a connector using a builtin builder, but you can implement the interface if you want.
We define a ```Install``` method which only returns an errors when installed.
You can retry to install your connector and see the error on the http response.

The ```Install``` method take a ```task.ConnectorContext``` parameter :
```go
type ConnectorContext interface {
	Context() context.Context
	Scheduler() Scheduler[TaskDescriptor]
}
```

Basically this context provides two things :
* a ```context.Context``` : If the connector make long-running processing, it should listen on this context to abort if necessary.
* a ```Scheduler```: A scheduler to run tasks

But, what is a task ?

A task is like a process that the framework will handle for you. It is basically a simple function.
When installed, a connector has the opportunity to schedule some tasks and let the system handle them for him.
A task has a descriptor.
The descriptor must be immutable and represents a specific task in the system. It can be anything.
A task also have a state. The state can change and the framework provides necessary apis to do that. We will come on that later.
As the descriptor, the state is freely defined by the connector.

In our case, the main task seems evident as we have to list the target repository.
Secondary tasks will be defined to read each files present in the directory.
We can define our task descriptor to a string. The value will be the file name in case of secondary tasks and a hardcoded value of "directory" for the main task.

Before add the logic, let's modify our previously introduced task descriptor :
```go
type (
    ...
    TaskDescriptor string
    ...
)

```

Add some logic on our connector :
```go
    ...
    WithInstall(func(ctx task.ConnectorContext) error {
        return ctx.Scheduler().Schedule("directory", true)
    }).
	...
```

Here we instruct the framework to create the task with the descriptor "directory".
Cool! The framework can handle the task, restart it, log/save errors etc...
But it doesn't know about the logic.

To do that, we have to use the last method of the connector : ```Resolve(descriptor TaskDescriptor) task.Task```
This method is in charge of providing a ```task.Task``` instance given a descriptor.

So, when calling ```ctx.Scheduler().Schedule("directory")```, the framework will call the ```Resolve``` method with "directory" as parameter.

Let's implement the resolve method :
```go
    ...
    WithInstall(func(ctx task.ConnectorContext) error {
        return ctx.Scheduler().Schedule("directory")
    }).
    WithResolve(func(descriptor models.TaskDescriptor) task.Task {
        if descriptor == "directory" {
			return func() {
			    // TODO
            }
        }
		// Secondary tasks
		return func() {
		    // TODO
        }
    }).
	...
```

Now, we have to implement the logic for each task.

Let's start with the main task which read the directory :
```go
    ...
    WithResolve(func(descriptor models.TaskDescriptor) task.Task {
        if descriptor == "directory" {
            return func(ctx context.Context, logget sharedlogging.Logger, scheduler task.Scheduler)
                for {
                    select {
                    case <-ctx.Done():
                        return nil
                    case <-time.After(10 * time.Second): // Could be configurable using Config object
                        logger.Infof("Opening directory '%s'...", config.Directory)
                        dir, err := os.ReadDir(config.Directory)
                        if err != nil {
                            logger.Errorf("Error opening directory '%s': %s", config.Directory, err)
                            continue
                        }

                        logger.Infof("Found %d files", len(dir))
                        for _, file := range dir {
                            err = scheduler.Schedule(TaskDescriptor(file.Name()))
                            if err != nil {
                                logger.Errorf("Error scheduling task '%s': %s", file.Name(), err)
                                continue
                            }
                        }
                    }
                }
            }
        }
		return func() error {
			return errors.New("not implemented")
        }
    }).
    ...
```

Let's test our implementation.

Start the server as usual and issue a curl request to install the connector :
```bash
curl http://localhost:8080/connectors/example -X POST -d '{"directory": "/tmp/payments"}'
```

Here we instruct the connector to watch the directory /tmp/payments. Check the app logs, you should see something like this :
```bash
payments-payments-1  | time="2022-07-01T12:29:05Z" level=info msg="Install connector example" component=connector-manager config="{/tmp/payments}" provider=example
payments-payments-1  | time="2022-07-01T12:29:05Z" level=info msg="Starting task..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T12:29:05Z" level=info msg="Connector installed" component=connector-manager provider=example
payments-payments-1  | time="2022-07-01T13:26:51Z" level=info msg="Opening directory '/tmp/payments'..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T13:26:51Z" level=error msg="Error opening directory '/tmp/payments': open /tmp/payments: no such file or directory" component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
```

As expected, the task trigger an error because of non-existent /tmp/payments directory.

You can see the tasks on api too :
```bash
curl http://localhost:8080/connectors/example/tasks | jq

[
  {
    "provider": "example",
    "descriptor": "directory",
    "createdAt": "2022-07-01T13:26:41.749Z",
    "status": "active",
    "error": "",
    "state": {},
    "id": "ImRpcmVjdG9yeSI="
  }
]
```

As you can see, a task has an id. This id is simply the descriptor of the task encoded in canonical json and encoded as base 64.

Let's create the missing directory:
```bash
docker compose exec payments mkdir /tmp/payments
```

After a few seconds, you should see thoses logs on app :
```bash
payments-payments-1  | time="2022-07-01T13:29:21Z" level=info msg="Opening directory '/tmp/payments'..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T13:29:21Z" level=info msg="Found 0 files" component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
```

Ok, create a payin file :
```bash
docker compose cp docs/samples-payin.json payments:/tmp/payments/001.json
```

You should see those lines on logs :
```bash
payments-payments-1  | time="2022-07-01T13:33:51Z" level=info msg="Opening directory '/tmp/payments'..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T13:33:51Z" level=info msg="Found 1 files" component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T13:33:52Z" level=info msg="Starting task..." component=scheduler provider=example task-id="IjAwMS5qc29uIg=="
payments-payments-1  | time="2022-07-01T13:33:52Z" level=error msg="Task terminated with error: not implemented" component=scheduler provider=example task-id="IjAwMS5qc29uIg=="
```

The log show our connector detect the file and trigger a new task for the file.
The task terminate with an error as the ```Resolve``` function does not handle the descriptor. We will do this later.

Again, you can view the tasks on the api :
```bash
[
  {
    "provider": "example",
    "descriptor": "directory",
    "createdAt": "2022-07-01T13:26:41.749Z",
    "status": "active",
    "error": "",
    "state": "XXX",
    "id": "ImRpcmVjdG9yeSI="
  },
  {
    "provider": "example",
    "descriptor": "001.json",
    "createdAt": "2022-07-01T13:33:31.935Z",
    "status": "failed",
    "error": "not implemented",
    "state": "XXX",
    "id": "IjAwMS5qc29uIg=="
  }
]
```

As you can see, as the first task is still active, the second is flagged as failed with an error message.

It's time to implement the second task :
```go
    ...
    file, err := os.Open(filepath.Join(config.Directory, string(descriptor)))
    if err != nil {
        return err
    }

    type JsonPayment struct {
        payments.Data
        Reference string `json:"reference"`
        Type string `json:"type"`
    }

    jsonPayment := &JsonPayment{}
    err = json.NewDecoder(file).Decode(jsonPayment)
    if err != nil {
        return err
    }

    return ingester.Ingest(ctx, ingestion.Batch{
        {
            Referenced: payments.Referenced{
                Reference: jsonPayment.Reference,
                Type:      jsonPayment.Type,
            },
            Payment:    &jsonPayment.Data,
            Forward:    true,
        },
    }, struct{}{})
    ...
```

Now restart the service, uninstall the connector, and reinstall it.

Here the logs :
```bash
payments-payments-1  | time="2022-07-01T14:25:20Z" level=info msg="Install connector example" component=connector-manager config="{/tmp/payments}" provider=example
payments-payments-1  | time="2022-07-01T14:25:20Z" level=info msg="Starting task..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T14:25:20Z" level=info msg="Connector installed" component=connector-manager provider=example
payments-payments-1  | time="2022-07-01T14:25:30Z" level=info msg="Opening directory '/tmp/payments'..." component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T14:25:30Z" level=info msg="Found 1 files" component=scheduler provider=example task-id="ImRpcmVjdG9yeSI="
payments-payments-1  | time="2022-07-01T14:25:30Z" level=info msg="Starting task..." component=scheduler provider=example task-id="IjAwMS5qc29uIg=="
payments-payments-1  | time="2022-07-01T14:25:30Z" level=info msg="Task terminated with success" component=scheduler provider=example task-id="IjAwMS5qc29uIg=="
```

As you can see, this time the second task has been started and was terminated with success.

It should have created a payment on database. Let's check :
```bash
curl http://localhost:8080/payments | jq

{
  "data": [
    {
      "id": "eyJwcm92aWRlciI6ImV4YW1wbGUiLCJyZWZlcmVuY2UiOiIwMDEiLCJ0eXBlIjoicGF5aW4ifQ==",
      "reference": "001",
      "type": "payin",
      "provider": "example",
      "status": "succeeded",
      "initialAmount": 100,
      "scheme": "",
      "asset": "USD",
      "createdAt": "0001-01-01T00:00:00Z",
      "raw": null,
      "adjustments": [
        {
          "status": "succeeded",
          "amount": 100,
          "date": "0001-01-01T00:00:00Z",
          "raw": null,
          "absolute": false
        }
      ]
    }
  ]
}
```

The last important part is the ```Ingester```.

In the code of the second task, you should have seen this part :
```go
return ingester.Ingest(ctx.Context(), ingestion.Batch{
    {
        Referenced: payments.Referenced{
            Reference: jsonPayment.Reference,
            Type:      jsonPayment.Type,
        },
        Payment:    &jsonPayment.Data,
        Forward:    true,
    },
}, struct{}{})
```
The ingester is in charge of accepting payments from a task and an eventual state to be persisted.

In our case, we don't alter the state, but we could if we want (we passed an empty struct).

If the connector is restarted, the task will be restarted with the previously state.

The complete code :
```go
package example

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/sharedlogging"
	payments "github.com/formancehq/payments/pkg"
	"github.com/formancehq/payments/pkg/bridge/ingestion"
	"github.com/formancehq/payments/pkg/bridge/integration"
	"github.com/formancehq/payments/pkg/bridge/task"
)

type (
	Config struct {
		Directory string
	}
	TaskDescriptor string
)

func (cfg Config) Validate() error {
	if cfg.Directory == "" {
		return errors.New("missing directory to watch")
	}
	return nil
}

var Loader = integration.NewLoaderBuilder[Config]("example").
	WithLoad(func(logger sharedlogging.Logger, config Config) integration.Connector {
		return integration.NewConnectorBuilder().
			WithInstall(func(ctx task.ConnectorContext) error {
				return ctx.Scheduler().Schedule("directory", false)
			}).
			WithResolve(func(descriptor models.TaskDescriptor) task.Task {
				if descriptor == "directory" {
					return func(ctx context.Context, logger sharedlogging.Logger, scheduler task.Scheduler) error {
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-time.After(10 * time.Second): // Could be configurable using Config object
								logger.Infof("Opening directory '%s'...", config.Directory)
								dir, err := os.ReadDir(config.Directory)
								if err != nil {
									logger.Errorf("Error opening directory '%s': %s", config.Directory, err)
									continue
								}

								logger.Infof("Found %d files", len(dir))
								for _, file := range dir {
									err = scheduler.Schedule(TaskDescriptor(file.Name()), false)
									if err != nil {
										logger.Errorf("Error scheduling task '%s': %s", file.Name(), err)
										continue
									}
								}
							}
						}
					}
				}
				return func(ctx context.Context, ingester ingestion.Ingester, resolver task.StateResolver) error {
					file, err := os.Open(filepath.Join(config.Directory, string(descriptor)))
					if err != nil {
						return err
					}

					type JsonPayment struct {
						payments.Data
						Reference string `json:"reference"`
						Type      string `json:"type"`
					}

					jsonPayment := &JsonPayment{}
					err = json.NewDecoder(file).Decode(jsonPayment)
					if err != nil {
						return err
					}

					return ingester.Ingest(ctx, ingestion.Batch{
						{
							Referenced: payments.Referenced{
								Reference: jsonPayment.Reference,
								Type:      jsonPayment.Type,
							},
							Payment: &jsonPayment.Data,
							Forward: true,
						},
					}, struct{}{})
				}
			}).
			Build()
	}).
	Build()
```
