package interfaces

type Consumer interface {
	Process(input []byte) (output string, err error)
}
