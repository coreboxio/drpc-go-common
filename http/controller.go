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

func (c *BaseController) Response(ctx *gin.Context, httpCode int, code Code, data interface{}) {
	ctx.JSON(httpCode, ResponseData{
		Code: code,
		Data: data,
	})
}

func (c *BaseController) Success(ctx *gin.Context, data interface{}) {
	c.Response(ctx, HTTP_OK, Code_OK, data)
}

func (c *BaseController) Error(ctx *gin.Context, code Code, data interface{}) {
	ctx.JSON(HTTP_FORBIDDEN, ResponseData{
		Code: code,
		Data: data,
	})
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
