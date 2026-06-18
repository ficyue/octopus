package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger
var atomicLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
var consoleEncoder = zapcore.EncoderConfig{
	TimeKey:       "time",
	LevelKey:      "level",
	MessageKey:    "msg",
	CallerKey:     "caller",
	StacktraceKey: "stacktrace",
	EncodeLevel:   zapcore.CapitalLevelEncoder,
	EncodeTime:    zapcore.RFC3339TimeEncoder,
	EncodeCaller:  zapcore.ShortCallerEncoder,
}

func init() {
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(consoleEncoder),
		zapcore.AddSync(os.Stdout),
		atomicLevel,
	)
	opts := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel),
	}
	Logger = zap.New(core, opts...).Sugar()
}

// EnableFileOutput adds a file writer alongside stdout.
func EnableFileOutput(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	// 同时输出到 stdout 和文件
	core := zapcore.NewTee(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(consoleEncoder),
			zapcore.AddSync(os.Stdout),
			atomicLevel,
		),
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(consoleEncoder),
			zapcore.AddSync(f),
			atomicLevel,
		),
	)
	opts := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel),
	}
	Logger = zap.New(core, opts...).Sugar()
	return nil
}

func SetLevel(level string) {
	var lvl zapcore.Level
	err := lvl.UnmarshalText([]byte(level))
	if err != nil {
		return
	}
	atomicLevel.SetLevel(lvl)
}

func Infof(template string, args ...interface{}) {
	Logger.Infof(template, args...)
}

func Errorf(template string, args ...interface{}) {
	Logger.Errorf(template, args...)
}

func Warnf(template string, args ...interface{}) {
	Logger.Warnf(template, args...)
}

func Debugf(template string, args ...interface{}) {
	Logger.Debugf(template, args...)
}
