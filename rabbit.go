package rabbit

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
	"strings"
	"errors"
	"strconv"

	logutils "github.com/aamendola/go-utils/log"
	"github.com/aamendola/go-utils/collections"
	"github.com/streadway/amqp"
)

// Consumer is the interface that you must implement if you want to consume messages
type Consumer interface {
	Process(timestamp time.Time, mesage Message) (bool, error)
}

// Client ...
type Client struct {
	uri             string
	exchange        string
	queue           string
	routingKeyFrom  string
	routingKeyTo    string
	blacklist       []string
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
func NewClient(host, user, password, exchange, queue, routingKeyFrom, routingKeyTo string, blacklist ...string) *Client {
	client := MakeClient(host, user, password, exchange, queue, routingKeyFrom, routingKeyTo, blacklist...)
	return &client
}

// MakeClient ...
func MakeClient(host, user, password, exchange, queue, routingKeyFrom, routingKeyTo string, blacklist ...string) Client {
	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host)
	if len(blacklist) > 1 {
		panic("The only optional parameter is 'blacklist'")
	}
	return Client{uri, exchange, queue, routingKeyFrom, routingKeyTo, blacklist}
}

// StartConsuming ...
func (c Client) StartConsuming(consumer Consumer) {

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

		keys := []string{c.routingKeyFrom}
		for _, key := range keys {
			log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", queue.Name, c.exchange, key)
			err = channel.QueueBind(
				queue.Name, // queue name
				key,        // routing key
				c.exchange, // exchange
				false,
				nil)
			logutils.Panic(err, "Failed to bind a queue")
		}

		// Declaro la cola del manejador de errores
		queue_errors_name := fmt.Sprintf("%s-errors", c.queue)
		routing_key_queue_errors := fmt.Sprintf("to-%s", queue_errors_name)

		errorsQueue, err := channel.QueueDeclare(
			queue_errors_name, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		logutils.Panic(err, "Failed to declare errors queue")
		showQueueInformation(errorsQueue)

		keys = []string{routing_key_queue_errors}
		for _, key := range keys {
			log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", errorsQueue.Name, c.exchange, key)
			err = channel.QueueBind(
				errorsQueue.Name, // queue name
				key,        // routing key
				c.exchange, // exchange
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
				log.Printf("[*] Receiving message [exchange:%s] [keys:%s] [body:%s]", c.exchange, keys, delivery.Body)
				// showDeliveryInformation(delivery)

				var dat map[string]interface{}
				err := json.Unmarshal(delivery.Body, &dat)
				logutils.Panic(err)

				message := Message{}
				json.Unmarshal(delivery.Body, &message)

				if c.blacklist != nil {
					exists := collections.Contains(c.blacklist, message.ID)
					if exists {
						delivery.Reject(false)
						log.Printf("[*] MessageId %s was rejected", message.ID)
						continue
					}
				}

				requeue, err := consumer.Process(delivery.Timestamp, message)
				if err != nil {
					next(delivery, channel, c.exchange, routing_key_queue_errors)
				} else if requeue {
					delivery.Nack(false, true)
					log.Printf("[*] Requeue message")
				} else {
					if c.routingKeyTo != "" {
						next(delivery, channel, c.exchange, c.routingKeyTo, )
					} else {
						log.Printf("[*] There is not need to send anything")
					}
				}
			}
		}()
		<-forever

	} else {

		// Declaro la cola propia de consumidor
		queue, err := channel.QueueDeclarePassive(
			c.queue, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
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

		forever := make(chan bool)
		go func() {
			time.Sleep(5 * time.Second)
			messagesCounter := 0
			for delivery := range deliveries {
				log.Printf("[*] Receiving message [exchange:%s] [body:%s]", c.exchange, delivery.Body)
				messagesCounter++
				// showDeliveryInformation(delivery)

				var dat map[string]interface{}
				err := json.Unmarshal(delivery.Body, &dat)
				logutils.Panic(err)

				message := Message{}
				json.Unmarshal(delivery.Body, &message)

				if c.blacklist != nil {
					justString := strings.Join(c.blacklist," ")
					log.Printf("[+] c.blacklist != nil: " + justString + "  message.ID: " + message.ID)
					log.Printf("[+] len(c.blacklist): " + len(c.blacklist))
					exists := collections.Contains(c.blacklist, message.ID)
					log.Printf("[+] exists: " + strconv.FormatBool(exists))
					if exists {
						delivery.Reject(false)
						log.Printf("[*] MessageId %s was rejected", message.ID)
						continue
					}
				} else {
					log.Printf("[+] c.blacklist == nil")
				}

				requeue, err := consumer.Process(delivery.Timestamp, message)
				if err != nil {
					errors.New("Error Consumer can not fails!")
				} else if requeue {
					delivery.Nack(false, true)
					log.Printf("[*] Requeue message")
				} else {
					if c.routingKeyTo != "" {
						next(delivery, channel, c.exchange, c.routingKeyTo, )
					} else {
						log.Printf("[*] There is not need to send anything")
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


func next(delivery amqp.Delivery, channel *amqp.Channel, exchange, routingKey string) {
	delivery.Ack(false)

	publishing := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         delivery.Body,
		DeliveryMode: amqp.Persistent,
		Timestamp: time.Now(),
	}
	mandatory := true
	immediate := false
	// showPublishingInformation(exchange, routingKey, mandatory, immediate, publishing)

	err := channel.Publish(
		exchange,          // exchange
		routingKey,        // routing key
		mandatory,         // mandatory
		immediate,         // immediate
		publishing,
	)
	logutils.Panic(err, "Failed to publish a message")

	log.Printf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", exchange, routingKey, delivery.Body)
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
