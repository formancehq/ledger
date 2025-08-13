package nats

// SubjectDetail contains jetstream subject detail (primary + all additional) along with durable and queue group names for a given watermill topic.
type SubjectDetail struct {
	Primary    string
	Additional []string
	QueueGroup string
}

// All combines the primary and all additional subjects for use by the jetstream client on creation.
func (s *SubjectDetail) All() []string {
	return append([]string{s.Primary}, s.Additional...)
}

// SubjectCalculator is a function used to calculate nats subject(s) for the given topic.
type SubjectCalculator func(queueGroupPrefix, topic string) *SubjectDetail

func DefaultSubjectCalculator(queueGroupPrefix, topic string) *SubjectDetail {
	return &SubjectDetail{
		Primary:    topic,
		QueueGroup: queueGroupPrefix,
	}
}
