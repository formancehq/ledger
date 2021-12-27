package lib

import (
	"github.com/sirupsen/logrus"
	"time"
)

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	logrus.Printf("%s took %s", name, elapsed)
}
