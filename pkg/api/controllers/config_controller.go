package controllers

import (
	"net/http"

	"github.com/swaggo/swag"

	"github.com/gin-gonic/gin"
	_ "github.com/numary/ledger/docs"
	"github.com/numary/ledger/pkg/config"
)

type LedgerLister interface {
	List() []string
}
type LedgerListerFn func() []string

func (fn LedgerListerFn) List() []string {
	return fn()
}

// ConfigController -
type ConfigController struct {
	BaseController
	Version       string
	StorageDriver string
	LedgerLister  LedgerLister
}

// NewConfigController -
func NewConfigController(version string, storageDriver string, lister LedgerLister) ConfigController {
	return ConfigController{
		Version:       version,
		StorageDriver: storageDriver,
		LedgerLister:  lister,
	}
}

// GetInfo godoc
// @Summary Server Info
// @Description Show server informations
// @Tags server
// @Schemes
// @Accept json
// @Produce json
// @Success 200 {object} config.ConfigInfo{}
// @Router /_info [get]
func (ctl *ConfigController) GetInfo(c *gin.Context) {
	ctl.response(
		c,
		http.StatusOK,
		config.ConfigInfo{
			Server:  "numary-ledger",
			Version: ctl.Version,
			Config: &config.Config{
				LedgerStorage: &config.LedgerStorage{
					Driver:  ctl.StorageDriver,
					Ledgers: ctl.LedgerLister.List(),
				},
			},
		},
	)
}

func (ctl *ConfigController) GetDocs(c *gin.Context) {
	doc, err := swag.ReadDoc("swagger")
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.Writer.Write([]byte(doc))
}
