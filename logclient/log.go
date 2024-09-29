package logclient

import (
	"github.com/rs/zerolog"
	"os"
	"strconv"
	"strings"
	"time"
)

var LogTarget = os.Stdout

const (
	gitlab = "gitlab"
	github = "github"
)

func NewJsonLogger(level zerolog.Level) zerolog.Logger {
	zerolog.SetGlobalLevel(level)

	// Lshortfile
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		if strings.Contains(file, github) {
			suffix := strings.Split(file, github)
			short = suffix[len(suffix)-1][1:]
		} else if strings.Contains(file, gitlab) {
			suffix := strings.Split(file, gitlab)
			short = suffix[len(suffix)-1][1:]
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}
	return zerolog.New(LogTarget).With().Timestamp().Caller().Logger()
}

func NewConsoleLogger(level zerolog.Level) zerolog.Logger {
	zerolog.SetGlobalLevel(level)

	// Lshortfile
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		if strings.Contains(file, github) {
			suffix := strings.Split(file, github)
			short = suffix[len(suffix)-1][1:]
		} else if strings.Contains(file, gitlab) {
			suffix := strings.Split(file, gitlab)
			short = suffix[len(suffix)-1][1:]
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	consoleWriter := zerolog.ConsoleWriter{
		Out:        LogTarget,
		NoColor:    false,
		TimeFormat: time.RFC3339,
	}

	l := zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()
	return l
}
