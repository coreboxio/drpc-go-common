package http

import (
	"io"

	"github.com/gin-gonic/gin"
)

var (
	router *gin.Engine
)

func Init() *gin.Engine {

	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard

	router = gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.Logger())
	router.ForwardedByClientIP = true
	router.Use(Cors())
	router.Use(Logger())

	return router
}

func Setup(f func(r *gin.Engine)) {
	f(router)
}
