package logging

import "context"

type Logger interface {
	Debugf(fmt string, args ...any)
	Infof(fmt string, args ...any)
	Errorf(fmt string, args ...any)
	Debug(args ...any)
	Info(args ...any)
	Error(args ...any)
	WithFields(map[string]any) Logger
	WithContext(ctx context.Context) Logger
}

func Debugf(fmt string, args ...any) {
	FromContext(context.TODO()).Debugf(fmt, args...)
}
func Infof(fmt string, args ...any) {
	FromContext(context.TODO()).Infof(fmt, args...)
}
func Errorf(fmt string, args ...any) {
	FromContext(context.TODO()).Errorf(fmt, args...)
}
func Debug(args ...any) {
	FromContext(context.TODO()).Debug(args...)
}
func Info(args ...any) {
	FromContext(context.TODO()).Info(args...)
}
func Error(args ...any) {
	FromContext(context.TODO()).Error(args...)
}
func WithFields(fields map[string]any) Logger {
	return FromContext(context.TODO()).WithFields(fields)
}
