package services

import (
	"github.com/numary/ledger/api/models"
	"github.com/spf13/viper"
)

// ConfigService -
type ConfigService struct {
}

// NewConfigService -
func NewConfigService() *ConfigService {
	return &ConfigService{}
}

// CreateConfigService -
func CreateConfigService() *ConfigService {
	return NewConfigService()
}

// GetConfig -
func (s *ConfigService) GetConfig() *models.Infos {
	return &models.Infos{
		Server:  "numary-ledger",
		Version: viper.Get("version"),
		Config: &models.Config{
			LedgerStorage: &models.LedgerStorage{
				Driver:  viper.Get("storage.driver"),
				Ledgers: viper.Get("ledgers"),
			},
		},
	}
}
