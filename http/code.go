package http

import HttpCode "net/http"

type Code string

type ErrorCode struct {
	HttpStatus int
	Code       Code
	Message    string
}

func (e ErrorCode) Error() string {
	return e.Message
}

var (
	// 成功
	Success       = ErrorCode{HttpStatus: HttpCode.StatusOK, Code: "api.success", Message: "success"}
	AuthFail      = ErrorCode{HttpStatus: HttpCode.StatusForbidden, Code: "api.unauthorized", Message: "unauthorized"}
	ParamError    = ErrorCode{HttpStatus: HttpCode.StatusForbidden, Code: "api.param_error", Message: "param error"}
	InternalError = ErrorCode{HttpStatus: HttpCode.StatusForbidden, Code: "api.internal_error", Message: "internal error"}
)
