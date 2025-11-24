package drpc

type Logger interface {
	Println(...any)
	Printf(string, ...any)
}
