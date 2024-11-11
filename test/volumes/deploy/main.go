package main

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	_ "embed"
	"encoding/pem"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/rds"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi-command/sdk/go/command/remote"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"golang.org/x/crypto/ssh"
	"io/fs"
	"maps"
	"math/rand"
	"os"
	"text/template"
)

var r = rand.New(rand.NewSource(1))

type stableReader struct{}

func (z stableReader) Read(p []byte) (n int, err error) {
	if len(p) == 1 {
		return 0, nil
	}

	return r.Read(p)
}

// todo: add build of driver image

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		subnet := config.Require(ctx, "aws-subnet-group-name")

		securityGroupIDS := config.Require(ctx, "aws-vpc-security-group-ids")

		subnetID := config.Require(ctx, "aws-subnet-id")

		userKeyPairName, _ := config.Try(ctx, "aws-keypair-name")

		rdsInstanceType, err := config.Try(ctx, "rds-instance-type")
		if err != nil {
			rdsInstanceType = string(rds.InstanceType_T3_Medium)
		}

		ledgerInstanceType, err := config.Try(ctx, "ledger-instance-type")
		if err != nil {
			ledgerInstanceType = string(ec2.InstanceType_T2_Small)
		}

		ledgerVersion, err := config.Try(ctx, "ledger-version")
		if err != nil {
			ledgerVersion = "latest"
		}

		driverVersion, err := config.Try(ctx, "driver-version")
		if err != nil {
			driverVersion = "latest"
		}

		awsRegion, err := config.Try(ctx, "aws-region")
		if err != nil {
			awsRegion = "eu-west-1"
		}

		deleteAllResources, _ := config.TryBool(ctx, "delete-all-resources")
		debug, _ := config.TryBool(ctx, "debug")

		privateKey, err := rsa.GenerateKey(&stableReader{}, 4096)
		if err != nil {
			return fmt.Errorf("generating private key: %w", err)
		}
		privDER := x509.MarshalPKCS1PrivateKey(privateKey)

		privBlock := pem.Block{
			Type:    "RSA PRIVATE KEY",
			Headers: nil,
			Bytes:   privDER,
		}

		privatePEM := pem.EncodeToMemory(&privBlock)

		publicRsaKey, err := ssh.NewPublicKey(privateKey.Public())
		if err != nil {
			return fmt.Errorf("generating public key: %w", err)
		}
		pubKeyBytes := ssh.MarshalAuthorizedKey(publicRsaKey)

		pair, err := ec2.NewKeyPair(ctx, "ledger-volumes-tests", &ec2.KeyPairArgs{
			KeyName:   pulumi.String("ledger-volumes-tests-" + ctx.Stack()),
			PublicKey: pulumi.String(pubKeyBytes),
		})
		if err != nil {
			return fmt.Errorf("creating key pair: %w", err)
		}

		user, err := iam.NewUser(ctx, "ledger-volumes-tests", &iam.UserArgs{
			Name:         pulumi.String("ledger-volumes-tests-" + ctx.Stack()),
			Path:         pulumi.String("/"),
			ForceDestroy: pulumi.BoolPtr(true),
		})
		if err != nil {
			return fmt.Errorf("creating user: %w", err)
		}

		lbRo, err := iam.GetPolicyDocument(ctx, &iam.GetPolicyDocumentArgs{
			Statements: []iam.GetPolicyDocumentStatement{
				{
					Effect: pulumi.StringRef("Allow"),
					Actions: []string{
						"rds:DescribeDBClusterSnapshots",
						"s3:GetObject",
						"s3:PutObject",
						"rds:CreateDBClusterSnapshot",
					},
					Resources: []string{
						"*",
					},
				},
			},
		}, nil)
		if err != nil {
			return err
		}

		_, err = iam.NewUserPolicy(ctx, "accesses", &iam.UserPolicyArgs{
			Name:   pulumi.String("ledger-volumes-tests-" + ctx.Stack()),
			User:   user.Name,
			Policy: pulumi.String(lbRo.Json),
		})
		if err != nil {
			return err
		}

		accessKey, err := iam.NewAccessKey(ctx, "ledger-volumes-tests", &iam.AccessKeyArgs{
			User: user.Name,
		})
		if err != nil {
			return fmt.Errorf("creating access key: %w", err)
		}

		steps := make([]uint64, 0)
		config.RequireObject(ctx, "steps", &steps)

		clusterInfo, err := rds.NewCluster(ctx, "default", &rds.ClusterArgs{
			DbSubnetGroupName:          pulumi.String(subnet),
			Engine:                     pulumi.String("aurora-postgresql"),
			EngineVersion:              pulumi.String("16"),
			SkipFinalSnapshot:          pulumi.Bool(true),
			MasterUsername:             pulumi.String("root"),
			MasterPassword:             pulumi.String("password"),
			ClusterIdentifier:          pulumi.String("ledger-volumes-tests-" + ctx.Stack()),
			PerformanceInsightsEnabled: pulumi.BoolPtr(true),
		})
		if err != nil {
			return fmt.Errorf("creating RDS cluster: %w", err)
		}

		primaryRDSInstance, err := rds.NewClusterInstance(ctx, "primary", &rds.ClusterInstanceArgs{
			ClusterIdentifier: clusterInfo.ClusterIdentifier,
			InstanceClass:     pulumi.String(rdsInstanceType),
			Engine:            pulumi.String("aurora-postgresql"),
			ApplyImmediately:  pulumi.BoolPtr(true),
			Tags: pulumi.StringMap{
				"Name": pulumi.String("primary"),
			},
		})
		if err != nil {
			return fmt.Errorf("creating RDS instance: %w", err)
		}

		setupLedgerInstance := pulumi.All(
			primaryRDSInstance.Endpoint,
			primaryRDSInstance.Port,
		).ApplyT(func(v []interface{}) (string, error) {
			endpoint := v[0].(string)
			port := v[1].(int)

			tpl := template.Must(template.New("setupEC2Ledger").Parse(setupEC2))
			buf := bytes.NewBuffer(nil)
			if err := tpl.Execute(buf, map[string]any{
				"PostgresURI":   fmt.Sprintf("postgres://root:password@%s:%d/postgres", endpoint, port),
				"LedgerVersion": ledgerVersion,
				"Debug":         debug,
			}); err != nil {
				return "", fmt.Errorf("executing template: %w", err)
			}

			return buf.String(), nil
		})

		ledgerInstance, err := ec2.NewInstance(ctx, "ledger", &ec2.InstanceArgs{
			Ami:          pulumi.String("ami-0715d656023fe21b4"), // debian-12-amd64-20240717-1811
			InstanceType: pulumi.String(ledgerInstanceType),
			SubnetId:     pulumi.String(subnetID),
			VpcSecurityGroupIds: pulumi.StringArray{
				pulumi.String(securityGroupIDS),
			},
			Tags: pulumi.StringMap{
				"Name": pulumi.String("ledger-high-volumes-tests-app"),
			},
			KeyName:                 pair.KeyName,
			UserData:                pulumi.Sprintf("%s", setupLedgerInstance),
			UserDataReplaceOnChange: pulumi.BoolPtr(true),
		})
		if err != nil {
			return fmt.Errorf("creating EC2 instance: %w", err)
		}

		bucket, err := s3.NewBucket(ctx, fmt.Sprintf("ledger-volumes-tests-%s-metrics", ctx.Stack()), &s3.BucketArgs{
			Acl: pulumi.String(s3.CannedAclPrivate),
			Tags: pulumi.StringMap{
				"Name":        pulumi.String("Ledger high volumes tests metrics"),
				"Environment": pulumi.String("Dev"),
			},
			ForceDestroy: pulumi.BoolPtr(deleteAllResources),
		})
		if err != nil {
			return err
		}

		setupDriverInstance := pulumi.All(
			ledgerInstance.PrivateIp,
			clusterInfo.ClusterIdentifier,
			bucket.Bucket,
			accessKey.ID(),
			accessKey.Secret,
		).
			ApplyT(func(v []any) (string, error) {
				ip := v[0].(string)
				clusterIdentifier := v[1].(string)
				bucket := v[2].(string)
				awsAccessKeyID := v[3].(pulumi.ID)
				awsSecretAccessKey := v[4].(string)

				tpl := template.Must(template.New("setupEC2Driver").Parse(setupEC2Driver))
				buf := bytes.NewBuffer(nil)

				if err := tpl.Execute(buf, map[string]any{
					"LedgerIP":            ip,
					"DriverVersion":       driverVersion,
					"DBClusterIdentifier": clusterIdentifier,
					"S3Bucket":            bucket,
					"Steps":               steps,
					"AwsRegion":           awsRegion,
					"AwsAccessKeyID":      awsAccessKeyID,
					"AwsSecretAccessKey":  awsSecretAccessKey,
				}); err != nil {
					return "", fmt.Errorf("executing template: %w", err)
				}

				return buf.String(), nil
			})

		driverInstance, err := ec2.NewInstance(ctx, "driver", &ec2.InstanceArgs{
			Ami:          pulumi.String("ami-0715d656023fe21b4"), // debian-12-amd64-20240717-1811
			InstanceType: pulumi.String(ledgerInstanceType),
			SubnetId:     pulumi.String(subnetID),
			VpcSecurityGroupIds: pulumi.StringArray{
				pulumi.String(securityGroupIDS),
			},
			Tags: pulumi.StringMap{
				"Name": pulumi.String("ledger-high-volumes-tests-driver"),
			},
			KeyName:  pair.KeyName,
			UserData: pulumi.Sprintf("%s", setupDriverInstance),
			//UserDataReplaceOnChange: pulumi.BoolPtr(true),
		})
		if err != nil {
			return fmt.Errorf("creating EC2 instance: %w", err)
		}

		userKeyPair, err := ec2.LookupKeyPair(ctx, &ec2.LookupKeyPairArgs{
			KeyName:          &userKeyPairName,
			IncludePublicKey: pointer.For(true),
		})
		if err != nil {
			return fmt.Errorf("looking up key pair: %w", err)
		}

		for _, instance := range []struct {
			Instance     *ec2.Instance
			Name         string
			TemplateData map[string]map[string]any
		}{
			{
				Instance: ledgerInstance,
				Name:     "ledger",
				TemplateData: map[string]map[string]any{
					".config/systemd/user/ledger.service": {
						"PostgresURI":   pulumi.Sprintf("postgres://root:password@%s:%d/postgres", primaryRDSInstance.Endpoint, primaryRDSInstance.Port),
						"LedgerVersion": pulumi.String(ledgerVersion),
						"Debug":         pulumi.Bool(debug),
					},
				},
			},
			{
				Instance: driverInstance,
				Name:     "driver",
			},
		} {
			_, err = installUserKeyPairToInstance(ctx, instance.Name, instance.Instance, userKeyPair, string(privatePEM))
			if err != nil {
				return fmt.Errorf("installing key pair to instance: %w", err)
			}

			_, err = createSystemDConfigDir(ctx, instance.Name, instance.Instance, string(privatePEM))
			if err != nil {
				return fmt.Errorf("creating systemd unit dir: %w", err)
			}

			err = fs.WalkDir(os.DirFS("./ec2"), "./ec2", func(path string, d fs.DirEntry, err error) error {
				if d.IsDir() {
					return nil
				}

				keys := make([]string, 0)
				values := make([]any, 0)
				for key := range maps.Keys(instance.TemplateData) {
					keys = append(keys, key)
					values = append(values, instance.TemplateData[key])
				}

				templatedFileContent := pulumi.
					All(values...).
					ApplyT(func(v []any) (string, error) {
						templateData := map[string]any{}
						for i, key := range keys {
							templateData[key] = v[i]
						}

						buf := bytes.NewBuffer(nil)
						err = template.Must(template.New("tpl").ParseFiles(path)).Execute(buf, templateData)
						if err != nil {
							return "", fmt.Errorf("executing template: %w", err)
						}

						return buf.String(), nil
					})

				_, err = remote.NewCommand(ctx, "ledger-systemd-unit", &remote.CommandArgs{
					Connection: remote.ConnectionArgs{
						Host:       instance.Instance.PrivateIp,
						User:       pulumi.String("admin"),
						PrivateKey: pulumi.String(privatePEM),
					},
					Create: pulumi.Sprintf(`cat > /home/admin/%s < EOF
%s
EOF`, path, templatedFileContent),
				}, pulumi.DependsOn([]pulumi.Resource{instance.Instance}))
				if err != nil {
					return fmt.Errorf("uploading ledger systemd unit file: %w", err)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}

		//if _, err := uploadLedgerSystemDUnitFile(
		//	ctx,
		//	pair,
		//	ledgerInstance,
		//	string(privatePEM),
		//	primaryRDSInstance,
		//	ledgerVersion,
		//	debug,
		//); err != nil {
		//	return fmt.Errorf("uploading ledger systemd unit file: %w", err)
		//}
		//
		//if _, err := uploadCollectorSystemDUnitFile(
		//	ctx,
		//	pair,
		//	ledgerInstance,
		//	string(privatePEM),
		//); err != nil {
		//	return fmt.Errorf("uploading ledger systemd unit file: %w", err)
		//}
		//
		//if _, err := uploadOtelCollectorConfig(ctx, pair, ledgerInstance, string(privatePEM)); err != nil {
		//	return fmt.Errorf("uploading otel collector config: %w", err)
		//}

		ctx.Export("driver-private-ip", driverInstance.PrivateIp)
		ctx.Export("db-instance-name", primaryRDSInstance.Identifier)
		ctx.Export("ledger-private-ip", ledgerInstance.PrivateIp)
		ctx.Export("metrics-bucket", bucket.Bucket)
		ctx.Export("db-endpoint", primaryRDSInstance.Endpoint)
		ctx.Export("public-key", pair.PublicKey)
		ctx.Export("private-key", pulumi.String(privatePEM))

		return nil
	})
}

func createSystemDConfigDir(ctx *pulumi.Context, name string, instance *ec2.Instance, privatePEM string) (*remote.Command, error) {
	return remote.NewCommand(ctx, "create-systemd-unit-dir-"+name, &remote.CommandArgs{
		Connection: remote.ConnectionArgs{
			Host:       instance.PrivateIp,
			User:       pulumi.String("admin"),
			PrivateKey: pulumi.String(privatePEM),
		},
		Create: pulumi.String("mkdir -p /home/admin/.config/systemd/user"),
	}, pulumi.DependsOn([]pulumi.Resource{instance}))
}

func installUserKeyPairToInstance(ctx *pulumi.Context, name string, instance *ec2.Instance, userKeyPair *ec2.LookupKeyPairResult, privatePEM string) (*remote.Command, error) {
	return remote.NewCommand(ctx, "add user key pair on instance "+name, &remote.CommandArgs{
		Connection: remote.ConnectionArgs{
			Host:       instance.PrivateIp,
			User:       pulumi.String("admin"),
			PrivateKey: pulumi.String(privatePEM),
		},
		Create: pulumi.String(`echo '` + userKeyPair.PublicKey + `' >> /home/admin/.ssh/authorized_keys && cat /home/admin/.ssh/authorized_keys`),
	}, pulumi.DependsOn([]pulumi.Resource{instance}))
}

////go:embed ec2/ledger/.config/systemd/user/ledger.service
//var ledgerSystemdUnit string

//func uploadLedgerSystemDUnitFile(ctx *pulumi.Context, pair *ec2.KeyPair, instance *ec2.Instance, privatePEM string, rdsInstance *rds.ClusterInstance, ledgerVersion string, debug bool) (*remote.CopyToRemote, error) {
//	return remote.NewCopyToRemote(ctx, "ledger-systemd-unit", &remote.CopyToRemoteArgs{
//		Connection: remote.ConnectionArgs{
//			Host:       instance.PrivateIp,
//			User:       pulumi.String("admin"),
//			PrivateKey: pulumi.String(privatePEM),
//		},
//		Source: pulumi.All(
//			rdsInstance.Endpoint,
//			rdsInstance.Port,
//		).ApplyT(func(v []interface{}) (pulumi.Asset, error) {
//			endpoint := v[0].(string)
//			port := v[1].(int)
//
//			unitFilePath := filepath.Join(os.TempDir(), fmt.Sprint(rand.Int())+".service")
//			f, err := os.Create(unitFilePath)
//			if err != nil {
//				return nil, fmt.Errorf("creating file: %w", err)
//			}
//			err = template.Must(template.New("ledger-systemd-unit").Parse(ledgerSystemdUnit)).Execute(f, map[string]any{
//				"PostgresURI":   fmt.Sprintf("postgres://root:password@%s:%d/postgres", endpoint, port),
//				"LedgerVersion": ledgerVersion,
//				"Debug":         debug,
//			})
//			if err != nil {
//				return nil, fmt.Errorf("executing template: %w", err)
//			}
//
//			return pulumi.NewFileAsset(unitFilePath), nil
//		}).(pulumi.AssetOutput),
//		RemotePath: pulumi.String("/home/admin/.config/systemd/user/ledger.service"),
//	}, pulumi.DependsOn([]pulumi.Resource{pair, instance}))
//}
//
//func uploadCollectorSystemDUnitFile(ctx *pulumi.Context, pair *ec2.KeyPair, instance *ec2.Instance, privatePEM string) (*remote.CopyToRemote, error) {
//	return remote.NewCopyToRemote(ctx, "otel-collector-systemd-unit", &remote.CopyToRemoteArgs{
//		Connection: remote.ConnectionArgs{
//			Host:       instance.PrivateIp,
//			User:       pulumi.String("admin"),
//			PrivateKey: pulumi.String(privatePEM),
//		},
//		Source:     pulumi.NewFileAsset("./systemd/otel-collector.service"),
//		RemotePath: pulumi.String("/home/admin/.config/systemd/user/otel-collector.service"),
//	}, pulumi.DependsOn([]pulumi.Resource{pair, instance}))
//}
//
//func uploadOtelCollectorConfig(ctx *pulumi.Context, pair *ec2.KeyPair, instance *ec2.Instance, privatePEM string) (*remote.CopyToRemote, error) {
//	return remote.NewCopyToRemote(ctx, "otel-collector-config", &remote.CopyToRemoteArgs{
//		Connection: remote.ConnectionArgs{
//			Host:       instance.PrivateIp,
//			User:       pulumi.String("admin"),
//			PrivateKey: pulumi.String(privatePEM),
//		},
//		Source:     pulumi.NewFileAsset("./config.yaml"),
//		RemotePath: pulumi.String("/home/admin/otel-collector-config.yaml"),
//	}, pulumi.DependsOn([]pulumi.Resource{pair, instance}))
//}

//go:embed setupEC2.sh
var setupEC2 string

var setupEC2Driver = setupEC2 + `

docker run -d \
	--restart on-failure \
	--name driver \
	--pull {{ if eq .DriverVersion "latest" }}always{{ else }}missing{{ end }} \
	-e AWS_REGION={{ .AwsRegion }} \
	-e AWS_ACCESS_KEY_ID="{{ .AwsAccessKeyID }}" \
	-e AWS_SECRET_ACCESS_KEY="{{ .AwsSecretAccessKey }}" \
	ghcr.io/formancehq/ledger-volumes-tests-driver:{{ .DriverVersion }} \
		--ledger-ip {{ .LedgerIP }} \
		{{- range .Steps }}
		--step {{ . }} \
		{{- end }}
		--vus 100 \
		--db-cluster-identifier {{ .DBClusterIdentifier }} \
		--s3-bucket {{ .S3Bucket }};
`
