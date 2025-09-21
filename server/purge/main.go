package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/service"
	"gopkg.in/yaml.v2"
	"os"
	"time"
)

type Config struct {
	Region   string `yaml:"region", envconfig:"AWS_REGION"`
	Database struct {
		Endpoint string `yaml:"endpoint", envconfig:"DYNAMODB_ENDPOINT"`
	} `yaml:"database"`
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func readFile(cfg *Config) {
	f, err := os.Open("config.yml")
	if err != nil {
		fmt.Println("Config file not found")
	} else {

		defer f.Close()

		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(cfg)
		if err != nil {
			processError(err)
		}
	}
}

func readEnv(cfg *Config) {
	err := envconfig.Process("", cfg)
	if err != nil {
		processError(err)
	}
}

func handleRequest(ctx context.Context) error {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	var cfg Config
	readFile(&cfg)
	readEnv(&cfg)
	log.Printf("%+v", cfg)

	var svc = service.NewService(cfg.Database.Endpoint, cfg.Region)
	/*
		err := svc.CreateTables()
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
	*/
	err := svc.Purge()
	if err != nil {
		logger.Fatal().Msg(err.Error())
	}

	return err
}

func main() {
	_, ok := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if ok {
		lambda.Start(handleRequest)
	} else {
		err := handleRequest(nil)
		if err != nil {
			return
		}
	}
}
