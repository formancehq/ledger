package worker

import (
	"github.com/robfig/cron/v3"
)

func mustParseCron(cronExpr string) cron.Schedule {
	schedule, err := cron.ParseStandard(cronExpr)
	if err != nil {
		panic("Invalid cron expression: " + cronExpr)
	}
	return schedule
}
