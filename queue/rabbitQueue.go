package queue

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/aamendola/rabbitmq-topics/customerror"
	"github.com/aamendola/rabbitmq-topics/interfaces"
	"github.com/streadway/amqp"
)

type RabbitQueue struct {
	rabbitHost     string
	rabbitUser     string
	rabbitPassword string
	rabbitExchange string
	routingKeyFrom string
	routingKeyTo   string
}
type Response2 struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	TraceID string `json:"traceId"`
}

/*
body:{
	"Id":"8d18675e-a677-4e14-9ae1-955877c5387e",
	"Type":"application/pdf",
	"Path":"/var/documents/2020/12/8d18675e-a677-4e14-9ae1-955877c5387e.pdf",
	"ImageUrl":null,
	"TraceId":"8d18675e-a677-4e14-9ae1-955877c5387e"
}
*/

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

func (rq RabbitQueue) Init(consumer interfaces.Consumer) {

	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", rq.rabbitUser, rq.rabbitPassword, rq.rabbitHost)
	conn, err := amqp.Dial(uri)
	customerror.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	customerror.FailOnError(err, "Failed to open a channel")
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
	customerror.FailOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	customerror.FailOnError(err, "Failed to declare a queue")

	keys := []string{rq.routingKeyFrom}

	for _, key := range keys {
		log.Printf("Biding [queue:%s] to [exchange:%s] with [routingKey:%s]", q.Name, rq.rabbitExchange, key)
		err = ch.QueueBind(
			q.Name,            // queue name
			key,               // routing key
			rq.rabbitExchange, // exchange
			false,
			nil)
		customerror.FailOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	customerror.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Receiving message [exchange:%s] [keys:%s] [body:%s]", rq.rabbitExchange, keys, d.Body)

			output, err := consumer.Process(d.Body)

			var dat map[string]interface{}
			if err := json.Unmarshal(d.Body, &dat); err != nil {
				panic(err)
			}

			fmt.Printf("==> dat %T %v\n", dat, dat)

			res := Response2{}
			json.Unmarshal(d.Body, &res)
			fmt.Println(res)
			fmt.Println(res.ID)
			fmt.Println(res.Path)
			fmt.Println(res.TraceID)

			fmt.Printf("==> res %T %v\n", res, res)
			fmt.Printf("==> res.id %T %v\n", res.ID, res.ID)
			fmt.Printf("==> res.Path %T %v\n", res.Path, res.Path)
			fmt.Printf("==> res.TraceID %T %v\n", res.TraceID, res.TraceID)

			customerror.FailOnError(err, "Failed to process body")

			if rq.routingKeyTo != "" {
				err = ch.Publish(
					rq.rabbitExchange, // exchange
					rq.routingKeyTo,   // routing key
					false,             // mandatory
					false,             // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(output),
					})
				customerror.FailOnError(err, "Failed to publish a message")

				log.Printf("Sending message [exchange:%s] [routingKey:%s] [body:%s]", rq.rabbitExchange, rq.routingKeyTo, output)
			} else {
				log.Printf("There is not need to send anything")
			}

		}
	}()

	log.Printf("[*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
