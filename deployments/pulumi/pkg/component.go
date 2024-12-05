package pulumi_ledger

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	appsv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apps/v1"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"slices"
	"time"
)

var ErrPostgresURIRequired = fmt.Errorf("postgresURI is required")

type UpgradeMode string

const (
	UpgradeModeDisabled UpgradeMode = "disabled"
	UpgradeModeJob      UpgradeMode = "job"
	UpgradeModeInApp    UpgradeMode = "in-app"
)

type Component struct {
	pulumi.ResourceState

	ServiceName        pulumix.Output[string]
	ServiceNamespace   pulumix.Output[string]
	ServicePort        pulumix.Output[int]
	ServiceInternalURL pulumix.Output[string]
}

type PostgresArgs struct {
	URI             pulumix.Input[string]
	AWSEnableIAM    pulumix.Input[bool]
	MaxIdleConns    pulumix.Input[*int]
	MaxOpenConns    pulumix.Input[*int]
	ConnMaxIdleTime pulumix.Input[*time.Duration]
}

type OtelTracesArgs struct {
	OtelTracesBatch                  pulumix.Input[bool]
	OtelTracesExporterFlag           pulumix.Input[string]
	OtelTracesExporterJaegerEndpoint pulumix.Input[string]
	OtelTracesExporterJaegerUser     pulumix.Input[string]
	OtelTracesExporterJaegerPassword pulumix.Input[string]
	OtelTracesExporterOTLPMode       pulumix.Input[string]
	OtelTracesExporterOTLPEndpoint   pulumix.Input[string]
	OtelTracesExporterOTLPInsecure   pulumix.Input[bool]
}

type OtelMetricsArgs struct {
	OtelMetricsExporterPushInterval               pulumix.Input[*time.Duration]
	OtelMetricsRuntime                            pulumix.Input[bool]
	OtelMetricsRuntimeMinimumReadMemStatsInterval pulumix.Input[*time.Duration]
	OtelMetricsExporter                           pulumix.Input[string]
	OtelMetricsKeepInMemory                       pulumix.Input[bool]
	OtelMetricsExporterOTLPMode                   pulumix.Input[string]
	OtelMetricsExporterOTLPEndpoint               pulumix.Input[string]
	OtelMetricsExporterOTLPInsecure               pulumix.Input[bool]
}

type OtelArgs struct {
	ResourceAttributes pulumix.Input[map[string]string]
	ServiceName        pulumix.Input[string]

	Traces  *OtelTracesArgs
	Metrics *OtelMetricsArgs
}

type ComponentArgs struct {
	Postgres                      PostgresArgs
	Otel                          *OtelArgs
	Namespace                     pulumix.Input[string]
	Timeout                       pulumix.Input[int]
	Tag                           pulumix.Input[string]
	ImagePullPolicy               pulumix.Input[string]
	Debug                         pulumix.Input[bool]
	ReplicaCount                  pulumix.Input[int]
	GracePeriod                   pulumix.Input[string]
	BallastSizeInBytes            pulumix.Input[int]
	NumscriptCacheMaxCount        pulumix.Input[int]
	BulkMaxSize                   pulumix.Input[int]
	BulkParallel                  pulumix.Input[int]
	TerminationGracePeriodSeconds pulumix.Input[*int]
	Upgrade                       pulumix.Input[UpgradeMode]

	ExperimentalFeatures             pulumix.Input[bool]
	ExperimentalNumscriptInterpreter pulumix.Input[bool]
}

func NewComponent(ctx *pulumi.Context, name string, args *ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	namespace := pulumix.Val[string]("")
	if args.Namespace != nil {
		namespace = args.Namespace.ToOutput(ctx.Context())
	}

	if args.Postgres.URI == nil {
		return nil, ErrPostgresURIRequired
	}
	postgresURI := pulumix.ApplyErr(args.Postgres.URI, func(postgresURI string) (string, error) {
		if postgresURI == "" {
			return "", ErrPostgresURIRequired
		}

		return postgresURI, nil
	})

	debug := pulumix.Val(true)
	if args.Debug != nil {
		debug = args.Debug.ToOutput(ctx.Context())
	}

	gracePeriod := pulumix.Val("0s")
	if args.GracePeriod != nil {
		gracePeriod = args.GracePeriod.ToOutput(ctx.Context())
	}

	tag := pulumix.Val("latest")
	if args.Tag != nil {
		tag = pulumix.Apply(args.Tag, func(tag string) string {
			if tag == "" {
				return "latest"
			}
			return tag
		})
	}
	ledgerImage := pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", tag)

	upgradeMode := UpgradeModeInApp
	if args.Upgrade != nil {
		var (
			upgradeModeChan = make(chan UpgradeMode, 1)
		)
		pulumix.ApplyErr(args.Upgrade, func(upgradeMode UpgradeMode) (any, error) {
			upgradeModeChan <- upgradeMode
			close(upgradeModeChan)
			return nil, nil
		})

		select {
		case <-ctx.Context().Done():
			return nil, ctx.Context().Err()
		case upgradeMode = <-upgradeModeChan:
			if upgradeMode == "" {
				upgradeMode = UpgradeModeInApp
			}
		}
	}

	if upgradeMode != "" && upgradeMode != UpgradeModeDisabled && upgradeMode != UpgradeModeJob && upgradeMode != UpgradeModeInApp {
		return nil, fmt.Errorf("invalid upgrade mode: %s", upgradeMode)
	}

	imagePullPolicy := pulumix.Val("")
	if args.ImagePullPolicy != nil {
		imagePullPolicy = args.ImagePullPolicy.ToOutput(ctx.Context())
	}

	envVars := corev1.EnvVarArray{
		corev1.EnvVarArgs{
			Name:  pulumi.String("POSTGRES_URI"),
			Value: postgresURI.Untyped().(pulumi.StringOutput),
		},
		corev1.EnvVarArgs{
			Name:  pulumi.String("BIND"),
			Value: pulumi.String(":8080"),
		},
		corev1.EnvVarArgs{
			Name:  pulumi.String("DEBUG"),
			Value: boolToString(debug).Untyped().(pulumi.StringOutput),
		},
	}

	if otel := args.Otel; otel != nil {
		if otel.ServiceName != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_SERVICE_NAME"),
				Value: otel.ServiceName.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			})
		}
		if otel.ResourceAttributes != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name: pulumi.String("OTEL_RESOURCE_ATTRIBUTES"),
				Value: pulumi.All(otel.ResourceAttributes).ApplyT(func(v []map[string]string) string {
					ret := ""
					keys := collectionutils.Keys(v[0])
					slices.Sort(keys)
					for _, key := range keys {
						ret += key + "=" + v[0][key] + ","
					}
					if len(ret) > 0 {
						ret = ret[:len(ret)-1]
					}
					return ret
				}).(pulumi.StringOutput),
			})
		}
		if traces := args.Otel.Traces; traces != nil {
			if traces.OtelTracesBatch != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_BATCH"),
					Value: boolToString(traces.OtelTracesBatch).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterFlag != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER"),
					Value: traces.OtelTracesExporterFlag.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterJaegerEndpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_ENDPOINT"),
					Value: traces.OtelTracesExporterJaegerEndpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterJaegerUser != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_USER"),
					Value: traces.OtelTracesExporterJaegerUser.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterJaegerPassword != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_PASSWORD"),
					Value: traces.OtelTracesExporterJaegerPassword.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterOTLPMode != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_MODE"),
					Value: traces.OtelTracesExporterOTLPMode.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterOTLPEndpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_ENDPOINT"),
					Value: traces.OtelTracesExporterOTLPEndpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OtelTracesExporterOTLPInsecure != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_INSECURE"),
					Value: boolToString(traces.OtelTracesExporterOTLPInsecure).Untyped().(pulumi.StringOutput),
				})
			}
		}

		if metrics := args.Otel.Metrics; metrics != nil {
			if metrics.OtelMetricsExporterPushInterval != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name: pulumi.String("OTEL_METRICS_EXPORTER_PUSH_INTERVAL"),
					Value: pulumix.Apply(metrics.OtelMetricsExporterPushInterval, func(pushInterval *time.Duration) string {
						if pushInterval == nil {
							return ""
						}
						return pushInterval.String()
					}).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsRuntime != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_RUNTIME"),
					Value: boolToString(metrics.OtelMetricsRuntime).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsRuntimeMinimumReadMemStatsInterval != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name: pulumi.String("OTEL_METRICS_RUNTIME_MINIMUM_READ_MEM_STATS_INTERVAL"),
					Value: pulumix.Apply(metrics.OtelMetricsRuntimeMinimumReadMemStatsInterval, func(interval *time.Duration) string {
						if interval == nil {
							return ""
						}
						return interval.String()
					}).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsExporter != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER"),
					Value: metrics.OtelMetricsExporter.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsKeepInMemory != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_KEEP_IN_MEMORY"),
					Value: boolToString(metrics.OtelMetricsKeepInMemory).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsExporterOTLPMode != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_MODE"),
					Value: metrics.OtelMetricsExporterOTLPMode.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsExporterOTLPEndpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_ENDPOINT"),
					Value: metrics.OtelMetricsExporterOTLPEndpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OtelMetricsExporterOTLPInsecure != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_INSECURE"),
					Value: boolToString(metrics.OtelMetricsExporterOTLPInsecure).Untyped().(pulumi.StringOutput),
				})
			}
		}
	}

	if args.BulkMaxSize != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("BULK_MAX_SIZE"),
			Value: pulumix.Apply(args.BulkMaxSize, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.BallastSizeInBytes != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("BALLAST_SIZE"),
			Value: pulumix.Apply(args.BallastSizeInBytes, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.BulkParallel != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("BULK_PARALLEL"),
			Value: pulumix.Apply(args.BulkParallel, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.NumscriptCacheMaxCount != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("NUMSCRIPT_CACHE_MAX_COUNT"),
			Value: pulumix.Apply(args.NumscriptCacheMaxCount, func(size int) string {
				if size == 0 {
					return ""
				}
				return fmt.Sprint(size)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.ExperimentalNumscriptInterpreter != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("EXPERIMENTAL_NUMSCRIPT_INTERPRETER"),
			Value: boolToString(args.ExperimentalNumscriptInterpreter).Untyped().(pulumi.StringOutput),
		})
	}

	if upgradeMode == UpgradeModeInApp {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("AUTO_UPGRADE"),
			Value: pulumi.String("true"),
		})
	}

	if args.ExperimentalFeatures != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("EXPERIMENTAL_FEATURES"),
			Value: boolToString(args.ExperimentalFeatures).Untyped().(pulumi.StringOutput),
		})
	}

	if args.GracePeriod != nil {
		// https://freecontent.manning.com/handling-client-requests-properly-with-kubernetes/
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("GRACE_PERIOD"),
			Value: gracePeriod.Untyped().(pulumi.StringOutput),
		})
	}

	if args.Postgres.AWSEnableIAM != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("POSTGRES_AWS_ENABLE_IAM"),
			Value: boolToString(args.Postgres.AWSEnableIAM).Untyped().(pulumi.StringOutput),
		})
	}

	if args.Postgres.ConnMaxIdleTime != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_CONN_MAX_IDLE_TIME"),
			Value: pulumix.Apply(args.Postgres.ConnMaxIdleTime, func(connMaxIdleTime *time.Duration) string {
				if connMaxIdleTime == nil {
					return ""
				}
				return connMaxIdleTime.String()
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.Postgres.MaxOpenConns != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_OPEN_CONNS"),
			Value: pulumix.Apply(args.Postgres.MaxOpenConns, func(maxOpenConns *int) string {
				if maxOpenConns == nil {
					return ""
				}
				return fmt.Sprint(*maxOpenConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	if args.Postgres.MaxIdleConns != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("POSTGRES_MAX_IDLE_CONNS"),
			Value: pulumix.Apply(args.Postgres.MaxIdleConns, func(maxIdleConns *int) string {
				if maxIdleConns == nil {
					return ""
				}
				return fmt.Sprint(*maxIdleConns)
			}).Untyped().(pulumi.StringOutput),
		})
	}

	terminationGracePeriodSeconds := pulumi.IntPtrFromPtr(nil)
	if args.TerminationGracePeriodSeconds != nil {
		terminationGracePeriodSeconds = args.TerminationGracePeriodSeconds.ToOutput(ctx.Context()).Untyped().(pulumi.IntPtrOutput)
	}

	deployment, err := appsv1.NewDeployment(ctx, "ledger", &appsv1.DeploymentArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
			Labels: pulumi.StringMap{
				"com.formance.stack/app": pulumi.String("ledger"),
			},
		},
		Spec: appsv1.DeploymentSpecArgs{
			Replicas: pulumi.Int(1),
			Selector: &metav1.LabelSelectorArgs{
				MatchLabels: pulumi.StringMap{
					"com.formance.stack/app": pulumi.String("ledger"),
				},
			},
			Template: &corev1.PodTemplateSpecArgs{
				Metadata: &metav1.ObjectMetaArgs{
					Labels: pulumi.StringMap{
						"com.formance.stack/app": pulumi.String("ledger"),
					},
				},
				Spec: corev1.PodSpecArgs{
					TerminationGracePeriodSeconds: terminationGracePeriodSeconds,
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name:            pulumi.String("ledger"),
							Image:           ledgerImage,
							ImagePullPolicy: imagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
							Args: pulumi.StringArray{
								pulumi.String("serve"),
							},
							Ports: corev1.ContainerPortArray{
								corev1.ContainerPortArgs{
									ContainerPort: pulumi.Int(8080),
									Name:          pulumi.String("http"),
									Protocol:      pulumi.String("TCP"),
								},
							},
							LivenessProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								FailureThreshold: pulumi.Int(1),
								PeriodSeconds:    pulumi.Int(10),
							},
							ReadinessProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								FailureThreshold: pulumi.Int(1),
								PeriodSeconds:    pulumi.Int(10),
							},
							StartupProbe: corev1.ProbeArgs{
								HttpGet: corev1.HTTPGetActionArgs{
									Path: pulumi.String("/_healthcheck"),
									Port: pulumi.String("http"),
								},
								FailureThreshold: pulumi.Int(60),
								PeriodSeconds:    pulumi.Int(5),
							},
							Env: envVars,
						},
					},
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	if upgradeMode == UpgradeModeJob {
		_, err = batchv1.NewJob(ctx, "migrate", &batchv1.JobArgs{
			Metadata: &metav1.ObjectMetaArgs{
				Namespace: namespace.Untyped().(pulumi.StringOutput),
			},
			Spec: batchv1.JobSpecArgs{
				Template: corev1.PodTemplateSpecArgs{
					Spec: corev1.PodSpecArgs{
						RestartPolicy: pulumi.String("OnFailure"),
						Containers: corev1.ContainerArray{
							corev1.ContainerArgs{
								Name: pulumi.String("migrate"),
								Args: pulumi.StringArray{
									pulumi.String("migrate"),
								},
								Image:           ledgerImage,
								ImagePullPolicy: imagePullPolicy.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
								Env: corev1.EnvVarArray{
									corev1.EnvVarArgs{
										Name:  pulumi.String("POSTGRES_URI"),
										Value: postgresURI.Untyped().(pulumi.StringOutput),
									},
									corev1.EnvVarArgs{
										Name:  pulumi.String("DEBUG"),
										Value: boolToString(debug).Untyped().(pulumi.StringOutput),
									},
								},
							},
						},
					},
				},
			},
		}, pulumi.Parent(cmp))
		if err != nil {
			return nil, err
		}
	}

	service, err := corev1.NewService(ctx, "ledger", &corev1.ServiceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Namespace: namespace.Untyped().(pulumi.StringOutput),
		},
		Spec: &corev1.ServiceSpecArgs{
			Selector: deployment.Spec.Selector().MatchLabels(),
			Type:     pulumi.String("ClusterIP"),
			Ports: corev1.ServicePortArray{
				corev1.ServicePortArgs{
					Port:       pulumi.Int(8080),
					TargetPort: pulumi.Int(8080),
					Protocol:   pulumi.String("TCP"),
					Name:       pulumi.String("http"),
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	cmp.ServiceName = pulumix.Apply(service.Metadata.Name().ToStringPtrOutput(), func(name *string) string {
		if name == nil {
			return ""
		}
		return *name
	})
	cmp.ServiceNamespace = pulumix.Apply(service.Metadata.Namespace().ToStringPtrOutput(), func(namespace *string) string {
		if namespace == nil {
			return ""
		}
		return *namespace
	})
	cmp.ServicePort = pulumix.Val(8080)
	cmp.ServiceInternalURL = pulumix.Apply(pulumi.Sprintf(
		"http://%s.%s.svc.cluster.local:%d",
		cmp.ServiceName,
		cmp.ServiceNamespace,
		cmp.ServicePort,
	), func(url string) string {
		return url
	})

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{
		"service-name":         cmp.ServiceName,
		"service-namespace":    cmp.ServiceNamespace,
		"service-port":         cmp.ServicePort,
		"service-internal-url": cmp.ServiceInternalURL,
	}); err != nil {
		return nil, fmt.Errorf("registering resource outputs: %w", err)
	}

	return cmp, nil
}

func boolToString(output pulumix.Input[bool]) pulumix.Output[string] {
	return pulumix.Apply(output, func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	})
}
