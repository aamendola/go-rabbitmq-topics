package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	logrus_stack "github.com/Gurpartap/logrus-stack"
	"github.com/sirupsen/logrus"
	"log"
	"strings"
	"time"

	"github.com/aamendola/go-utils/collections"
	logutils "github.com/aamendola/go-utils/log"
	"github.com/streadway/amqp"
)

var logger *logrus.Logger

func Log(eventID string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"eventId": eventID,
	}).Info(args...)
}

func configureLogger(logLevel, logFormat string) {
	logger = logrus.New()

	if logLevel == "" {
		logLevel = "info"
	}
	logger.Level, _ = logrus.ParseLevel(logLevel)

	if logFormat == "" {
		logFormat = "json"
	}

	if strings.EqualFold(logFormat, "json") {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else if strings.EqualFold(logFormat, "plain") {
		logger.SetFormatter(&logrus.TextFormatter{})
	} else {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	logger.AddHook(logrus_stack.StandardHook())
}

// Consumer is the interface that you must implement if you want to consume messages
type Consumer interface {
	Process(timestamp time.Time, mesage Message) (bool, error)
}

// Client ...
type Client struct {
	uri            string
	exchange       string
	queue          string
	routingKeyFrom string
	routingKeyTo   string
	blacklist      []string
}

// Message ...
type Message struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	TraceID  string `json:"traceId"`
	Type     string `json:"type"`
	ImageURL string `json:"ImageURL"`
	Url      string `json:"url"`
}

// NewClient ...
func NewClient(logLevel, logFormat, host, user, password, exchange, queue, routingKeyFrom, routingKeyTo, blacklist string) *Client {
	client := MakeClient(logLevel, logFormat, host, user, password, exchange, queue, routingKeyFrom, routingKeyTo, blacklist)
	return &client
}

// MakeClient ...
func MakeClient(logLevel, logFormat, host, user, password, exchange, queue, routingKeyFrom, routingKeyTo, blacklist string) Client {
	configureLogger(logLevel, logFormat)
	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host)
	return Client{uri, exchange, queue, routingKeyFrom, routingKeyTo, strings.Split(blacklist, ",")}
}

// StartConsuming ...
func (c Client) StartConsuming(consumer Consumer) {
	time.Sleep(5 * time.Second)

	conn, err := amqp.Dial(c.uri)
	logutils.Panic(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	logutils.Panic(err, "Failed to open a channel")
	defer channel.Close()

	err = channel.ExchangeDeclare(
		c.exchange, // name
		"topic",    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	logutils.Panic(err, "Failed to declare an exchange")

	errorProcessor := strings.Contains(c.queue, "-errors")

	if !errorProcessor {
		// Declaro la cola propia de consumidor
		queue, err := channel.QueueDeclare(
			c.queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		logutils.Panic(err, "Failed to declare a queue")
		showQueueInformation(queue)

		// Declaro la cola del manejador de errores
		queue_errors_name := fmt.Sprintf("%s-errors", c.queue)
		routing_key_queue_errors := fmt.Sprintf("to-%s", queue_errors_name)

		keys := []string{c.routingKeyFrom}
		for _, key := range keys {
			log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKeyFrom:%s] [routingKeyTo:%s] [routingKeyErrors:%s]", queue.Name, c.exchange, key, c.routingKeyTo, routing_key_queue_errors)
			err = channel.QueueBind(
				queue.Name, // queue name
				key,        // routing key
				c.exchange, // exchange
				false,
				nil)
			logutils.Panic(err, "Failed to bind a queue")
		}

		errorsQueue, err := channel.QueueDeclare(
			queue_errors_name, // name
			true,              // durable
			false,             // delete when unused
			false,             // exclusive
			false,             // no-wait
			nil,               // arguments
		)
		logutils.Panic(err, "Failed to declare errors queue")
		showQueueInformation(errorsQueue)

		keys = []string{routing_key_queue_errors}
		for _, key := range keys {
			log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", errorsQueue.Name, c.exchange, key)
			err = channel.QueueBind(
				errorsQueue.Name, // queue name
				key,              // routing key
				c.exchange,       // exchange
				false,
				nil)
			logutils.Panic(err, "Failed to bind errors queue")
		}

		// Empiezo a consumir los mensajes
		deliveries, err := channel.Consume(
			queue.Name, // queue
			"",         // consumer
			false,      // auto ack
			false,      // exclusive
			false,      // no local
			false,      // no wait
			nil,        // args
		)
		logutils.Panic(err, "Failed to register a consumer")

		forever := make(chan bool)
		go func() {
			for delivery := range deliveries {
				var dat map[string]interface{}
				err := json.Unmarshal(delivery.Body, &dat)
				logutils.Panic(err)

				message := Message{}
				json.Unmarshal(delivery.Body, &message)

				Log(message.ID, fmt.Sprintf("[*] Receiving message [exchange:%s] [keys:%s] [body:%s]", c.exchange, keys, delivery.Body))
				// showDeliveryInformation(delivery)

				deleted := deleteMessageFromBlackList(message, c.blacklist, delivery)
				if !deleted {
					requeue, err := consumer.Process(delivery.Timestamp, message)
					if err != nil {
						next(message, delivery, channel, c.exchange, routing_key_queue_errors)
					} else if requeue {
						delivery.Nack(false, true)
						Log(message.ID, fmt.Sprintf("[*] Requeue message"))
					} else {
						if c.routingKeyTo != "" {
							next(message, delivery, channel, c.exchange, c.routingKeyTo)
						} else {
							delivery.Ack(false)
							Log(message.ID, fmt.Sprintf("[*] There is not need to send anything"))
						}
					}
				}
			}
		}()
		<-forever

	} else {

		// Declaro la cola propia de consumidor
		queue, err := channel.QueueDeclarePassive(
			c.queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)

		// Empiezo a consumir los mensajes
		deliveries, err := channel.Consume(
			queue.Name, // queue
			"",         // consumer
			false,      // auto ack
			false,      // exclusive
			false,      // no local
			false,      // no wait
			nil,        // args
		)
		logutils.Panic(err, "Failed to register a consumer")

		log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKeyFrom:%s] [routingKeyTo:%s]", c.queue, c.exchange, c.routingKeyFrom, c.routingKeyTo)

		forever := make(chan bool)
		go func() {
			messagesCounter := 0
			for delivery := range deliveries {
				messagesCounter++
				// showDeliveryInformation(delivery)

				var dat map[string]interface{}
				err := json.Unmarshal(delivery.Body, &dat)
				logutils.Panic(err)

				message := Message{}
				json.Unmarshal(delivery.Body, &message)

				Log(message.ID, fmt.Sprintf("[*] Receiving message [exchange:%s] [body:%s]", c.exchange, delivery.Body))

				deleted := deleteMessageFromBlackList(message, c.blacklist, delivery)
				if !deleted {
					requeue, err := consumer.Process(delivery.Timestamp, message)
					if err != nil {
						errors.New("Error Consumer can not fails!")
					} else if requeue {
						delivery.Nack(false, true)
						Log(message.ID, fmt.Sprintf("[*] Requeue message"))
					} else {
						if c.routingKeyTo != "" {
							next(message, delivery, channel, c.exchange, c.routingKeyTo)
						} else {
							delivery.Ack(false)
							Log(message.ID, fmt.Sprintf("[*] There is not need to send anything"))
						}
					}
				}

				if queue.Messages == messagesCounter {
					log.Printf("[*] All messages were processed")
					forever <- true
				}
			}
		}()

		if queue.Messages == 0 {
			log.Printf("[*] There is no messages in the queue. Bye!")
		} else {
			log.Printf("[*] Waiting for logs. To exit press CTRL+C")
			<-forever
		}
	}

}

func next(message Message, delivery amqp.Delivery, channel *amqp.Channel, exchange, routingKey string) {
	delivery.Ack(false)

	publishing := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         delivery.Body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}
	mandatory := true
	immediate := false
	// showPublishingInformation(exchange, routingKey, mandatory, immediate, publishing)

	err := channel.Publish(exchange, routingKey, mandatory, immediate, publishing)
	logutils.Panic(err, "Failed to publish a message")

	Log(message.ID, fmt.Sprintf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", exchange, routingKey, delivery.Body))
}

func deleteMessageFromBlackList(message Message, blacklist []string, delivery amqp.Delivery) bool {
	deleted := false
	if blacklist != nil {
		exists := collections.Contains(blacklist, message.ID)
		if exists {
			delivery.Reject(false)
			deleted = true
			Log(message.ID, fmt.Sprintf("[*] Message rejected"))
		}
	}
	return deleted
}

func showDeliveryInformation(delivery amqp.Delivery) {
	log.Printf("=== [Delivery Information] ==========================\n")
	log.Printf("CorrelationId ................ %s\n", delivery.CorrelationId)
	log.Printf("ReplyTo ...................... %s\n", delivery.ReplyTo)
	log.Printf("Expiration ................... %s\n", delivery.Expiration)
	log.Printf("MessageId .................... %s\n", delivery.MessageId)
	log.Printf("Timestamp .................... %s\n", delivery.Timestamp)
	log.Printf("Type ......................... %s\n", delivery.Type)
	log.Printf("UserId ....................... %s\n", delivery.UserId)
	log.Printf("ConsumerTag .................. %s\n", delivery.ConsumerTag)
	log.Printf("MessageCount ................. %c\n", delivery.MessageCount)
	log.Printf("DeliveryTag .................. %d\n", delivery.DeliveryTag)
	log.Printf("Redelivered .................. %t\n", delivery.Redelivered)
	log.Printf("Exchange ..................... %s\n", delivery.Exchange)
	log.Printf("RoutingKey ................... %s\n", delivery.RoutingKey)
	log.Printf("======================================================\n")
}

func showPublishingInformation(exchange, routingKeyTo string, mandatory, immediate bool, publishing amqp.Publishing) {
	log.Printf("=== [Publishing Information] =========================\n")
	log.Printf("Exchange ......................... %s\n", exchange)
	log.Printf("RoutingKeyTo ..................... %s\n", routingKeyTo)
	log.Printf("mandatory ........................ %t\n", mandatory)
	log.Printf("immediate ........................ %t\n", immediate)
	log.Printf("------------------------------------------------------\n")
	log.Printf("ContentType ...................... %s\n", publishing.ContentType)
	log.Printf("ContentEncoding .................. %s\n", publishing.ContentEncoding)
	log.Printf("DeliveryMode ..................... %d\n", publishing.DeliveryMode)
	log.Printf("Priority ......................... %d\n", publishing.Priority)
	log.Printf("CorrelationId .................... %s\n", publishing.CorrelationId)
	log.Printf("ReplyTo .......................... %s\n", publishing.ReplyTo)
	log.Printf("Expiration ....................... %s\n", publishing.Expiration)
	log.Printf("MessageId ........................ %s\n", publishing.MessageId)
	log.Printf("Timestamp ........................ %s\n", publishing.Timestamp)
	log.Printf("Type ............................. %s\n", publishing.Type)
	log.Printf("UserId ........................... %s\n", publishing.UserId)
	log.Printf("AppId ............................ %s\n", publishing.AppId)
	log.Printf("======================================================\n")
}

func showQueueInformation(queue amqp.Queue) {
	log.Printf("=== [Queue Information] ==============================\n")
	log.Printf("Name ............................. %s\n", queue.Name)
	log.Printf("Messages ......................... %d\n", queue.Messages)
	log.Printf("Consumers ........................ %d\n", queue.Consumers)
	log.Printf("======================================================\n")
}
