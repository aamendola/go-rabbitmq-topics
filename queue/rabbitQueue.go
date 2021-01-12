package queue

import (
	"encoding/json"
	"fmt"
	"log"

	utils "github.com/aamendola/go-utils"
	"github.com/streadway/amqp"
)

type Consumer interface {
	Process(mesage Message) (err error)
}

type RabbitQueue struct {
	rabbitHost     string
	rabbitUser     string
	rabbitPassword string
	rabbitExchange string
	routingKeyFrom string
	routingKeyTo   string
}

type Message struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	TraceID string `json:"traceId"`
}

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
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
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
		true,   // auto ack ??????????????????????????????????????????
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Receiving message [exchange:%s] [keys:%s] [body:%s]", rq.rabbitExchange, keys, d.Body)

			var dat map[string]interface{}
			if err := json.Unmarshal(d.Body, &dat); err != nil {
				panic(err)
			}

			fmt.Printf("==> dat %T %v\n", dat, dat)

			message := Message{}
			json.Unmarshal(d.Body, &message)
			fmt.Println(message)
			fmt.Println(message.ID)
			fmt.Println(message.Path)
			fmt.Println(message.TraceID)

			fmt.Printf("==> message %T %v\n", message, message)
			fmt.Printf("==> message.id %T %v\n", message.ID, message.ID)
			fmt.Printf("==> message.Path %T %v\n", message.Path, message.Path)
			fmt.Printf("==> message.TraceID %T %v\n", message.TraceID, message.TraceID)

			err := consumer.Process(message)
			utils.FailOnError(err, "Failed to process body")

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
