package tunnel

import (
	"os"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.Out = os.Stdout
	log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	level := os.Getenv("TUNNEL_LOG")
	if level == "" {
		log.Level = logrus.WarnLevel
		return
	}
	parsed, err := logrus.ParseLevel(level)
	if err != nil {
		log.Level = logrus.WarnLevel
		log.Warnf("invalid TUNNEL_LOG level %q", level)
		return
	}
	log.Level = parsed
}
