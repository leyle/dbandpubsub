package logclient

import (
	"github.com/leyle/go-crud-helper/mongoclient"
	"github.com/rs/zerolog"
	"testing"
)

func TestNewJsonLogger(t *testing.T) {
	logger := NewJsonLogger(zerolog.InfoLevel)
	logger.Info().Str("reqId", mongoclient.GetObjectId()).Msg("hello world")
}

func TestNewConsoleLogger(t *testing.T) {
	logger := NewConsoleLogger(zerolog.InfoLevel)
	logger.Info().Str("reqId", mongoclient.GetObjectId()).Msg("hello world")

	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	logger.Info().Msg("info msg level")
	logger.Warn().Msg("warn msg level")
}

func TestHTTPLogMiddleware(t *testing.T) {

}
