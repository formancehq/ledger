package licence

import (
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
)

type Licence struct {
	logger logging.Logger

	jwtToken            string
	licenceValidateTick time.Duration
	appStoped           chan struct{}

	// Will be checked in claims (aud claim)
	serviceName string
	// kube-system namespace UID, should be in the sub claim
	clusterID string
	// Expected issuer, should be in the iss claim
	expectedIssuer string
}

func NewLicence(
	logger logging.Logger,
	jwtToken string,
	licenceValidateTick time.Duration,
	serviceName string,
	clusterID string,
	expectedIssuer string,
) *Licence {
	return &Licence{
		logger:              logger,
		jwtToken:            jwtToken,
		licenceValidateTick: licenceValidateTick,
		serviceName:         serviceName,
		clusterID:           clusterID,
		expectedIssuer:      expectedIssuer,
		appStoped:           make(chan struct{}),
	}
}

func (l *Licence) run(licenceError chan error) {
	l.licenceValidateTick = 2 * time.Minute
	ticker := time.NewTicker(l.licenceValidateTick)

	for {
		select {
		case <-l.appStoped:
			l.logger.Info("Licence check stopped, app stopped")
			// App is stopped, return
			return

		case <-ticker.C:
			l.logger.Info("Licence check started")
			if err := l.validate(); err != nil {
				l.logger.Error("Licence check failed", err)
				licenceError <- err
				return
			}
			l.logger.Info("Licence check passed")
		}
	}
}

func (l *Licence) Start(licenceError chan error) error {
	// First check before launching the goroutine
	l.logger.Info("Licence check started")
	if err := l.validate(); err != nil {
		l.logger.Errorf("Licence check failed %v", err)
		licenceError <- err
		return err
	}
	l.logger.Info("Licence check passed")

	go l.run(licenceError)

	return nil
}

func (l *Licence) Stop() {
	close(l.appStoped)
}
