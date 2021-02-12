package queuer

import (
	"github.com/aamendola/go-rabbitmq-topics/rabbit"
)

// Queuer ...
type Queuer interface {
	StartConsuming(consumer rabbit.Consumer)
}
