package http

type Code int

const (
	Code_OK Code = 0

	Code_AuthFail      Code = 1
	Code_ParamError    Code = 2
	Code_InternalError Code = 3
)
