package http

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

type Server struct {
	logger        logging.Logger
	cluster       service.MasterCluster
}

// NewServer creates a new server instance (used by handlers)
func NewServer(logger logging.Logger, cluster service.MasterCluster) *Server {
	return &Server{
		logger:        logger,
		cluster:       cluster,
	}
}
