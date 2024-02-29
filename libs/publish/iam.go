package publish

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

type MSKAccessTokenProvider struct {
	region      string
	roleArn     string
	sessionName string
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthTokenFromRole(context.TODO(), m.region, m.roleArn, m.sessionName)
	return &sarama.AccessToken{Token: token}, err
}
