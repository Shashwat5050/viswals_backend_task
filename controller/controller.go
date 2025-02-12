package controller

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	defaultHttpPort = "8080"
)

type Controller struct {
	logger         *zap.Logger
	corsMiddleware gin.HandlerFunc
	// usecase        IConsumerService
	httpMux        *http.ServeMux
	httpPort       string
}

func (c *Controller) setDefaults() {

	if c.httpPort == "" {
		c.httpPort = defaultHttpPort
	}
}

type Option func(*Controller)

// WithCorsMiddleware add the cors middleware to existing controllers
func WithCorsMiddleware(corsMiddleware gin.HandlerFunc) func(*Controller) {
	return func(c *Controller) {
		c.corsMiddleware = corsMiddleware
	}
}

func WithHttpMux(httpMux *http.ServeMux) func(*Controller) {
	return func(c *Controller) {
		c.httpMux = httpMux
	}
}

func WithHttpPort(port string) func(*Controller) {
	return func(c *Controller) {
		c.httpPort = port
	}
}

// func New(usecase IConsumerService, opts ...Option) *Controller {
// 	ac := &Controller{
// 		usecase: usecase,
// 	}

// 	for _, opt := range opts {
// 		opt(ac)
// 	}

// 	// set default options

// 	return ac
// }

func (c *Controller) Start() error {
	c.registerRoutes()

	server := &http.Server{
		Handler:           c.httpMux,
		Addr:              fmt.Sprintf(":%v", c.httpPort),
		ReadHeaderTimeout: 3 * time.Second,
	}

	return server.ListenAndServe()
}

func (c *Controller) registerRoutes() {
	router := gin.Default()

	// define cors middleware if provided
	if c.corsMiddleware != nil {
		router.Use(c.corsMiddleware)
	}

	// routes := router.Group("")
	// {
	// 	// routes.GET("/users", c.GetAllUsers)
	// 	// routes.GET("/users/:id", c.GetUserById)
	// }

	// c.httpMux.Handle("/", router)
}
