package rabbit

import (
	"encoding/json"
	"fmt"
	"log"

	utils "github.com/aamendola/go-utils"
	"github.com/streadway/amqp"
)

// Consumer is the interface that you must implement if you want to consume messages
type Consumer interface {
	Process(mesage Message) error
}

// Client ...
type Client struct {
	uri            string
	exchange       string
	routingKeyFrom string
	routingKeyTo   string
}

// Message ...
type Message struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	TraceID  string `json:"traceId"`
	Type     string `json:"type"`
	ImageURL string `json:"ImageURL"`
}

// NewClient ...
func NewClient(host, user, password, exchange, routingKeyFrom, routingKeyTo string) *Client {
	client := new(Client)
	client.uri = fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host)
	client.exchange = exchange
	client.routingKeyFrom = routingKeyFrom
	client.routingKeyTo = routingKeyTo
	return client
}

// MakeClient ...
func MakeClient(host, user, password, exchange, routingKeyFrom, routingKeyTo string) Client {
	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host)
	return Client{uri, exchange, routingKeyFrom, routingKeyTo}
}

// StartConsuming ...
func (c Client) StartConsuming(consumer Consumer) {

	conn, err := amqp.Dial(c.uri)
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
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
	utils.FailOnError(err, "Failed to declare an exchange")

	queue, err := channel.QueueDeclare(
		"stanley", // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	utils.FailOnError(err, "Failed to declare a queue")
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
		utils.FailOnError(err, "Failed to bind a queue")
	}

	deliveries, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto ack
		false,      // exclusive
		false,      // no local
		false,      // no wait
		nil,        // args
	)
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	go func() {
		for delivery := range deliveries {

			log.Printf("\n[*] Receiving message [exchange:%s] [keys:%s] [body:%s]", c.exchange, keys, delivery.Body)
			showDeliveryInformation(delivery)

			var dat map[string]interface{}
			err := json.Unmarshal(delivery.Body, &dat)
			utils.PanicOnError(err)

			message := Message{}
			json.Unmarshal(delivery.Body, &message)

			err = consumer.Process(message)
			utils.FailOnError(err, "Failed to process body")
			//d.Nack(true, true)
			delivery.Ack(false)

			if c.routingKeyTo != "" {

				publishing := amqp.Publishing{
					ContentType:  "text/plain",
					Body:         delivery.Body,
					DeliveryMode: amqp.Persistent,
				}
				mandatory := true
				immediate := false
				showPublishingInformation(c.exchange, c.routingKeyTo, mandatory, immediate, publishing)

				err = channel.Publish(
					c.exchange,     // exchange
					c.routingKeyTo, // routing key
					mandatory,      // mandatory
					immediate,      // immediate
					publishing,
				)
				utils.FailOnError(err, "Failed to publish a message")

				log.Printf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", c.exchange, c.routingKeyTo, delivery.Body)
			} else {
				log.Printf("There is not need to send anything")
			}

		}
	}()

	log.Printf("[*] Waiting for logs. To exit press CTRL+C")
	<-forever
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
