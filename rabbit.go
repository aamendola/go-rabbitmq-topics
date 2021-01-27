package rabbit

import (
	"encoding/json"
	"fmt"
	"log"

	utils "github.com/aamendola/go-utils"
	"github.com/streadway/amqp"
)

// Consumer ...
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

	msgs, err := ch.Consume(
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
		for d := range msgs {
			log.Printf("[*] Receiving message [exchange:%s] [keys:%s] [body:%s]", c.exchange, keys, d.Body)

			var dat map[string]interface{}
			err := json.Unmarshal(d.Body, &dat)
			utils.PanicOnError(err)

			message := Message{}
			json.Unmarshal(d.Body, &message)

			err = consumer.Process(message)
			utils.FailOnError(err, "Failed to process body")
			//d.Nack(true, true)
			d.Ack(false)

			if c.routingKeyTo != "" {
				err = ch.Publish(
					c.exchange,     // exchange
					c.routingKeyTo, // routing key
					true,           // mandatory
					false,          // immediate
					amqp.Publishing{
						ContentType:  "text/plain",
						Body:         d.Body,
						DeliveryMode: 2,
					})
				utils.FailOnError(err, "Failed to publish a message")

				log.Printf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", c.exchange, c.routingKeyTo, d.Body)
			} else {
				log.Printf("There is not need to send anything")
			}

		}
	}()

	log.Printf("[*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
