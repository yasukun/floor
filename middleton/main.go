package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis"
	"github.com/labstack/echo"
	"github.com/labstack/gommon/log"
	"github.com/linkedin/goavro"
	"github.com/yasukun/roure/middleton/lib"
)

// Usage ...
func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

// SelectCodec ...
func SelectCodec(name string) (codec *goavro.Codec, err error) {
	schema, err := Asset(name)
	if err != nil {
		return
	}
	codec, err = goavro.NewCodec(string(schema))
	if err != nil {
		return
	}
	return
}

func main() {
	flag.Usage = Usage
	addr := flag.String("addr", ":1323", "listen to server address")
	confname := flag.String("c", "middleton.toml", "path to config")
	flag.Parse()

	conf, err := lib.DecodeConfigToml(*confname)
	if err != nil {
		log.Printf("decode config error %v\n", err)
		os.Exit(1)
	}

	subjectCodec, err := SelectCodec(conf.Subject.Schema)
	if err != nil {
		log.Printf("select codec error %v\n", err)
		os.Exit(1)
	}
	codecs := lib.Codecs{Subject: subjectCodec}

	client := redis.NewClient(&redis.Options{
		Addr:     conf.Ledisdb.Addr,
		Password: conf.Ledisdb.Password,
		DB:       conf.Ledisdb.DB,
	})

	// Setup
	e := echo.New()

	e.Use(func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cc := &lib.CustomContext{c, conf, client, codecs}
			return h(cc)
		}
	})

	e.Logger.SetLevel(log.INFO)

	lib.Routes(e)

	// Start server
	go func() {
		if err := e.Start(*addr); err != nil {
			e.Logger.Info("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 10 seconds.
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}
