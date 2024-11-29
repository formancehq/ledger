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
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed ec2/setup.sh
var setupEC2 string

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

		deleteAllResources := config.GetBool(ctx, "delete-all-resources")
		debug := config.GetBool(ctx, "debug")
		otlpSignozAccessToken := config.Get(ctx, "otlp-signoz-access-token")
		otlpSignozEndpoint := config.Get(ctx, "otlp-signoz-endpoint")

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
			KeyName:  pair.KeyName,
			UserData: pulumi.String(setupEC2),
			//UserDataReplaceOnChange: pulumi.BoolPtr(true),
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
			UserData: pulumi.String(setupEC2),
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
					".config/otel-collector/config.yaml": {
						"OTLPSignozAccessToken": pulumi.String(otlpSignozAccessToken),
						"OTLPSignozEndpoint":    pulumi.String(otlpSignozEndpoint),
					},
				},
			},
			{
				Instance: driverInstance,
				Name:     "driver",
				TemplateData: map[string]map[string]any{
					".config/systemd/user/driver.service": {
						"LedgerIP":            ledgerInstance.PrivateIp,
						"DriverVersion":       driverVersion,
						"DBClusterIdentifier": clusterInfo.ClusterIdentifier,
						"S3Bucket":            bucket.Bucket,
						"Steps":               steps,
						"AwsRegion":           awsRegion,
						"AwsAccessKeyID":      accessKey.ID(),
						"AwsSecretAccessKey":  accessKey.Secret,
					},
				},
			},
		} {
			_, err = installUserKeyPairToInstance(ctx, instance.Name, instance.Instance, userKeyPair, string(privatePEM))
			if err != nil {
				return fmt.Errorf("installing key pair to instance: %w", err)
			}

			instanceCopiedFiles := make([]pulumi.Resource, 0)
			err = filepath.WalkDir("./ec2/"+instance.Name, func(path string, d fs.DirEntry, err error) error {
				if d.IsDir() {
					return nil
				}

				targetPath := strings.TrimPrefix(path, "ec2/"+instance.Name+"/")

				templatedFileContent := pulumi.
					All(instance.TemplateData[targetPath]).
					ApplyT(func(v []any) (string, error) {
						fileContent, err := os.ReadFile(path)
						if err != nil {
							return "", fmt.Errorf("reading file: %w", err)
						}

						buf := bytes.NewBuffer(nil)
						err = template.Must(template.New("tpl-"+path).
							Parse(string(fileContent))).
							Execute(buf, v[0])
						if err != nil {
							return "", fmt.Errorf("executing template for %s: %w", path, err)
						}

						return buf.String(), nil
					})

				ret, err := remote.NewCommand(ctx, "systemd-unit "+instance.Name+" "+targetPath, &remote.CommandArgs{
					Connection: remote.ConnectionArgs{
						Host:       instance.Instance.PrivateIp,
						User:       pulumi.String("admin"),
						PrivateKey: pulumi.String(privatePEM),
					},
					Create: pulumi.Sprintf(`mkdir -p /home/admin/%s && cat <<EOF > /home/admin/%s
%s
EOF`, filepath.Dir(targetPath), targetPath, templatedFileContent),
				})
				if err != nil {
					return fmt.Errorf("uploading ledger systemd unit file: %w", err)
				}

				instanceCopiedFiles = append(instanceCopiedFiles, ret)

				return nil
			})
			if err != nil {
				return err
			}

			_, err = remote.NewCommand(ctx, "systemd-unit "+instance.Name+" restart services", &remote.CommandArgs{
				Connection: remote.ConnectionArgs{
					Host:       instance.Instance.PrivateIp,
					User:       pulumi.String("admin"),
					PrivateKey: pulumi.String(privatePEM),
				},
				Create: pulumi.Sprintf(`
					pushd .config/systemd/user;
					systemctl --user enable *.service;
					systemctl --user daemon-reload;
					systemctl --user restart *.service;
				`),
			}, pulumi.DependsOn(instanceCopiedFiles))
			if err != nil {
				return fmt.Errorf("uploading ledger systemd unit file: %w", err)
			}
		}

		ctx.Export("driver-private-ip", driverInstance.PrivateIp)
		ctx.Export("db-instance-name", primaryRDSInstance.Identifier)
		ctx.Export("ledger-private-ip", ledgerInstance.PrivateIp)
		ctx.Export("metrics-bucket", bucket.Bucket)
		ctx.Export("db-endpoint", primaryRDSInstance.Endpoint)
		ctx.Export("public-key", pair.PublicKey)

		return nil
	})
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
