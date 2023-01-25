package stripe

type TaskDescriptor struct {
	Name    string `json:"name" yaml:"name" bson:"name"`
	Main    bool   `json:"main,omitempty" yaml:"main" bson:"main"`
	Account string `json:"account,omitempty" yaml:"account" bson:"account"`
}
