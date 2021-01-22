package queue

import (
	"encoding/json"
	"fmt"
	"log"

	utils "github.com/aamendola/go-utils"
	"github.com/streadway/amqp"
)

// Consumer ...
type Consumer interface {
	Process(mesage Message) (err error)
}

// RabbitQueue ...
type RabbitQueue struct {
	rabbitHost     string
	rabbitUser     string
	rabbitPassword string
	rabbitExchange string
	routingKeyFrom string
	routingKeyTo   string
}

// Message ...
type Message struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	TraceID string `json:"traceId"`
	Type    string `json:"type"`
}

// NewRabbitQueue ...
func NewRabbitQueue(rabbitHost, rabbitUser, rabbitPassword, rabbitExchange, routingKeyFrom, routingKeyTo string) *RabbitQueue {
	rq := new(RabbitQueue)
	rq.rabbitHost = rabbitHost
	rq.rabbitUser = rabbitUser
	rq.rabbitPassword = rabbitPassword
	rq.rabbitExchange = rabbitExchange
	rq.routingKeyFrom = routingKeyFrom
	rq.routingKeyTo = routingKeyTo
	return rq
}

// MakeRabbitQueue ...
func MakeRabbitQueue(rabbitHost, rabbitUser, rabbitPassword, rabbitExchange, routingKeyFrom, routingKeyTo string) RabbitQueue {
	return RabbitQueue{rabbitHost, rabbitUser, rabbitPassword, rabbitExchange, routingKeyFrom, routingKeyTo}
}

func (rq RabbitQueue) Init(consumer Consumer) {

	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", rq.rabbitUser, rq.rabbitPassword, rq.rabbitHost)
	conn, err := amqp.Dial(uri)
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		rq.rabbitExchange, // name
		"topic",           // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	utils.FailOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"azulado", // name
		true,      // durable
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	utils.FailOnError(err, "Failed to declare a queue")

	keys := []string{rq.routingKeyFrom}

	for _, key := range keys {
		log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", q.Name, rq.rabbitExchange, key)
		err = ch.QueueBind(
			q.Name,            // queue name
			key,               // routing key
			rq.rabbitExchange, // exchange
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
			log.Printf("\n") // Borrar esta linea!
			log.Printf("Receiving message [exchange:%s] [keys:%s] [body:%s]", rq.rabbitExchange, keys, d.Body)

			var dat map[string]interface{}
			err := json.Unmarshal(d.Body, &dat)
			utils.PanicOnError(err)

			message := Message{}
			json.Unmarshal(d.Body, &message)

			//log.Printf("==> message %T %v\n", message, message)
			//log.Printf("==> message.id %T %v\n", message.ID, message.ID)
			//log.Printf("==> message.Path %T %v\n", message.Path, message.Path)
			//log.Printf("==> message.TraceID %T %v\n", message.TraceID, message.TraceID)

			err = consumer.Process(message)
			utils.FailOnError(err, "Failed to process body")
			//d.Nack(true, true)
			log.Printf("############################### ACK ###############################")
			d.Ack(false)

			if rq.routingKeyTo != "" {
				err = ch.Publish(
					rq.rabbitExchange, // exchange
					rq.routingKeyTo,   // routing key
					false,             // mandatory
					false,             // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        d.Body,
					})
				utils.FailOnError(err, "Failed to publish a message")

				log.Printf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", rq.rabbitExchange, rq.routingKeyTo, d.Body)
			} else {
				log.Printf("There is not need to send anything")
			}

		}
	}()

	log.Printf("[*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
