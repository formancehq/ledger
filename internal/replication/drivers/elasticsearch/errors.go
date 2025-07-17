package elasticsearch

type errIncorrectIAMConfiguration struct{}

func (e errIncorrectIAMConfiguration) Error() string {
	return "incorrect IAM configuration: username and password should not be set when IAM is enabled"
}

func (e errIncorrectIAMConfiguration) Is(target error) bool {
	_, ok := target.(errIncorrectIAMConfiguration)
	return ok
}

func newErrIncorrectIAMConfiguration() error {
	return errIncorrectIAMConfiguration{}
}
