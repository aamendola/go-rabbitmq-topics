package queuer

// Message ...
type Message struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	TraceID  string `json:"traceId"`
	Type     string `json:"type"`
	ImageURL string `json:"ImageURL"`
}

// Consumer is the interface that you must implement if you want to consume messages
type Consumer interface {
	Process(mesage Message) error
}

// Queuer ...
type Queuer interface {
	StartConsuming(consumer Consumer)
}
