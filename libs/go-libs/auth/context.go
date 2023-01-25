package auth

import "context"

type contextKey string

func WithAgent(ctx context.Context, agent Agent) context.Context {
	return context.WithValue(ctx, contextKey("agent"), agent)
}

func AgentFromContext(ctx context.Context) Agent {
	agent := ctx.Value(contextKey("agent"))
	if agent == nil {
		return nil
	}
	return agent.(Agent)
}
