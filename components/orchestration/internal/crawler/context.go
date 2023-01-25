package crawler

type Context struct {
	Variables map[string]string
}

func (ctx Context) WithVariables(variables map[string]string) Context {
	m := make(map[string]string)
	for key, value := range ctx.Variables {
		m[key] = value
	}
	for key, value := range variables {
		m[key] = value
	}
	ctx.Variables = m
	return ctx
}

func NewContext() Context {
	return Context{
		Variables: map[string]string{},
	}
}
