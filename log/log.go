package log

import (
	"os"
	"time"

	cesstash "cmps/pkg/cesstash"

	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var Logger logr.Logger

func init() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	zlogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.Stamp}).With().Timestamp().Caller().Logger()
	Logger = zerologr.New(&zlogger)

	cesstash.SetLogger(Logger)
}
