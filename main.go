package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"code.cloudfoundry.org/gorouter/common/uuid"
	"code.cloudfoundry.org/gorouter/config"
	goRouterLogger "code.cloudfoundry.org/gorouter/logger"
	"code.cloudfoundry.org/localip"
	"github.com/cloudfoundry/gorouter/common"
	"github.com/cloudfoundry/gorouter/mbus"
	"github.com/ghodss/yaml"
	"github.com/mailru/easyjson"
	"github.com/nats-io/nats.go"
	"github.com/uber-go/zap"
)

var (
	c                config.Config
	gorouterYamlPath = "/var/vcap/jobs/gorouter/config/gorouter.yml"
	logger           goRouterLogger.Logger
)

func init() {

	var logLevel zap.Level
	logLevel.UnmarshalText([]byte("DEBUG"))
	timestampFormat := "unix-epoch"
	logger = goRouterLogger.NewLogger("gorouter-nats-debugger", timestampFormat, logLevel, zap.Output(os.Stdout))

	gc := os.Getenv("GOROUTER_CONFIG_YAML")
	if gc != "" {
		gorouterYamlPath = gc
	}

	c = config.Config{}
	b, err := ioutil.ReadFile(gorouterYamlPath)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(b, &c)
	if err != nil {
		panic(err)
	}
	c.NatsClientPingInterval = 20 * time.Second
	c.NatsClientMessageBufferSize = 131072

}

func startMessage(guid string) ([]byte, error) {
	host, err := localip.LocalIP()
	if err != nil {
		return nil, err
	}

	d := common.RouterStart{
		Id:    fmt.Sprintf("SUPPORT-%s", guid),
		Hosts: []string{host},
	}
	message, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func main() {

	natsReconnected := make(chan mbus.Signal)
	natsClient := mbus.Connect(&c, natsReconnected, logger.Session("nats"))

	guid, err := uuid.GenerateUUID()
	if err != nil {
		logger.Fatal("failed-to-generate-uuid", zap.Error(err))
	}
	m, err := startMessage(guid)
	err = natsClient.Publish("router.start", m)
	if err != nil {
		logger.Fatal("failed to send start message", zap.Error(err))
	}

	_, err = natsClient.Subscribe("router.greet", func(msg *nats.Msg) {
		response, _ := startMessage(guid)
		_ = natsClient.Publish(msg.Reply, response)
	})

	if err != nil {
		logger.Fatal("Failed to subscribe to greet", zap.Error(err))
	}
	_, err = subscribeRoutes(natsClient)
	if err != nil {
		logger.Fatal("Failed to subscribe to routes", zap.Error(err))
	}

	// stay up forever
	for {
		time.Sleep(1 * time.Second)
	}

}

func createRegistryMessage(data []byte) (*mbus.RegistryMessage, error) {
	var msg mbus.RegistryMessage

	jsonErr := easyjson.Unmarshal(data, &msg)
	if jsonErr != nil {
		return nil, jsonErr
	}

	if !msg.ValidateMessage() {
		return nil, errors.New("Unable to validate message. route_service_url must be https")
	}

	return &msg, nil
}

func printJsonMSG(msg *mbus.RegistryMessage) {
	b, err := json.Marshal(msg)
	if err != nil {
		logger.Debug("printJsonMSG", zap.String("data", fmt.Sprintf("%v", msg)))
		logger.Error("marshal-registry-message-failed", zap.Error(err))
		return
	}
	logger.Info("resigstry-message", zap.String("data", fmt.Sprintf("%s", b)))
}

func subscribeRoutes(natsClient *nats.Conn) (*nats.Subscription, error) {
	natsSubscription, err := natsClient.Subscribe("router.*", func(message *nats.Msg) {
		msg, regErr := createRegistryMessage(message.Data)
		if regErr != nil {
			logger.Error("validation-error",
				zap.Error(regErr),
				zap.String("payload", string(message.Data)),
				zap.String("subject", message.Subject),
			)
			return
		}
		printJsonMSG(msg)
	})

	if err != nil {
		return nil, err
	}

	err = natsSubscription.SetPendingLimits(c.NatsClientMessageBufferSize, c.NatsClientMessageBufferSize*1024)
	if err != nil {
		return nil, fmt.Errorf("subscriber: SetPendingLimits: %s", err)
	}

	return natsSubscription, nil
}
