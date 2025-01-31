package omnipaxos

import (
	"os"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SetupLogger configures the global logger with the specified level and pretty printing.
func SetupLogger(logLevel int, prettyPrint bool) {
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))
	if prettyPrint {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

func (op *OmniPaxos) Trace(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Trace().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Debug(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Debug().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Info(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Info().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Warn(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Warn().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Error(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Error().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Fatal(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Fatal().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}

func (op *OmniPaxos) Panic(format string, a ...interface{}) (n int, err error) {
	enableLogging := atomic.LoadInt32(&op.enableLogging)
	if enableLogging == 1 {
		log.Panic().Msgf("[SERVER=%d] "+format, append([]interface{}{op.me}, a...)...)
	}
	return
}
