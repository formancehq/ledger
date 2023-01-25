package workflow

const (
	waitEventSignalName = "wait-event"
)

type event struct {
	Event string `json:"event"`
}

func NewEmptyEvent() event {
	return event{}
}
