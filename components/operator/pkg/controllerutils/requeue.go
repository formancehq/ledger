package controllerutils

import (
	"time"

	controllerruntime "sigs.k8s.io/controller-runtime"
)

func Requeue(after ...time.Duration) *controllerruntime.Result {
	if len(after) > 1 {
		panic("too many arguments")
	}

	return &controllerruntime.Result{
		Requeue: true,
		RequeueAfter: func() time.Duration {
			if len(after) == 1 {
				return after[0]
			}
			return 0
		}(),
	}
}
