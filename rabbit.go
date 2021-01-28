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

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		c.exchange, // name
		"topic",    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	utils.FailOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	utils.FailOnError(err, "Failed to declare a queue")

	keys := []string{c.routingKeyFrom}

	for _, key := range keys {
		log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", q.Name, c.exchange, key)
		err = ch.QueueBind(
			q.Name,     // queue name
			key,        // routing key
			c.exchange, // exchange
			false,
			nil)
		utils.FailOnError(err, "Failed to bind a queue")
	}

	deliveries, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	go func() {
		for delivery := range deliveries {
			log.Printf("\n[*] Receiving message [exchange:%s] [keys:%s] [body:%s]", c.exchange, keys, delivery.Body)

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
				err = ch.Publish(
					c.exchange,     // exchange
					c.routingKeyTo, // routing key
					true,           // mandatory
					false,          // immediate
					amqp.Publishing{
						ContentType:  "text/plain",
						Body:         delivery.Body,
						DeliveryMode: 2,
					})
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
	log.Printf("======================================================\n")
	log.Printf("CorrelationId ................%s\n", delivery.CorrelationId)
	log.Printf("ReplyTo ................%s\n", delivery.ReplyTo)
	log.Printf("Expiration ................%s\n", delivery.Expiration)
	log.Printf("MessageId ................%s\n", delivery.MessageId)
	log.Printf("Timestamp ................%s\n", delivery.Timestamp)
	log.Printf("Type ................%s\n", delivery.Type)
	log.Printf("UserId ................%s\n", delivery.UserId)
	log.Printf("ConsumerTag ................%s\n", delivery.ConsumerTag)
	log.Printf("MessageCount ................%s\n", delivery.MessageCount)
	log.Printf("DeliveryTag ................%s\n", delivery.DeliveryTag)
	log.Printf("Redelivered ................%s\n", delivery.Redelivered)
	log.Printf("Exchange ................%s\n", delivery.Exchange)
	log.Printf("RoutingKey ................%s\n", delivery.RoutingKey)
	log.Printf("======================================================\n")
	/*
		Acknowledger Acknowledger // the channel from which this delivery arrived

		Headers Table // Application or header exchange table

		// Properties
		ContentType     string    // MIME content type
		ContentEncoding string    // MIME content encoding
		DeliveryMode    uint8     // queue implementation use - non-persistent (1) or persistent (2)
		Priority        uint8     // queue implementation use - 0 to 9
		CorrelationId   string    // application use - correlation identifier
		ReplyTo         string    // application use - address to reply to (ex: RPC)
		Expiration      string    // implementation use - message expiration spec
		MessageId       string    // application use - message identifier
		Timestamp       time.Time // application use - message timestamp
		Type            string    // application use - message type name
		UserId          string    // application use - creating user - should be authenticated user
		AppId           string    // application use - creating application id

		// Valid only with Channel.Consume
		ConsumerTag string

		// Valid only with Channel.Get
		MessageCount uint32

		DeliveryTag uint64
		Redelivered bool
		Exchange    string // basic.publish exchange
		RoutingKey  string // basic.publish routing key

		Body []byte
	*/
}