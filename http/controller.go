package http

import (
	"github.com/gin-gonic/gin"
)

type BaseController struct {
}

type ResponseData struct {
	Code Code        `json:"code"`
	Data interface{} `json:"data"`
}

const (
	HTTP_OK        = 200
	HTTP_FORBIDDEN = 403
)

func (c *BaseController) Response(ctx *gin.Context, code ErrorCode, data interface{}) {
	ctx.JSON(code.HttpStatus, ResponseData{
		Code: code.Code,
		Data: data,
	})
}

func (c *BaseController) Success(ctx *gin.Context, data interface{}) {
	c.Response(ctx, Success, data)
}

func (c *BaseController) Error(ctx *gin.Context, err error) {
	c.ErrorWithData(ctx, err, nil)
}

func (c *BaseController) ErrorWithData(ctx *gin.Context, err error, data interface{}) {
	code, ok := err.(ErrorCode)
	if ok {
		httpStatus := HTTP_FORBIDDEN
		if code.HttpStatus > 0 {
			httpStatus = code.HttpStatus
		}
		c.Response(ctx, ErrorCode{HttpStatus: httpStatus, Code: code.Code}, data)
	} else {
		c.Response(ctx, InternalError, data)
	}
}

func (c *BaseController) GetUserAgent(ctx *gin.Context) *UserAgentEntity {
	ua, ok := ctx.Get("ua")
	if !ok {
		return nil
	}

	if ua == nil {
		return nil
	}

	userAgentEntity := ua.(*UserAgentEntity)

	if userAgentEntity == nil {
		return nil
	}

	return userAgentEntity
}
