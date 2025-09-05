package main

import (
	"context"
	"encoding/json"
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

func handleRequest(ctx context.Context, event json.RawMessage) ([]service.RecommendedEvent, error) {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	var cfg Config
	readFile(&cfg)
	readEnv(&cfg)
	logger.Info().Msgf("%+v", cfg)

	var svc = service.NewService(cfg.Database.Endpoint, cfg.Region)

	var matchingRequest service.MatchingRequest
	if err := json.Unmarshal(event, &matchingRequest); err != nil {
		logger.Error().Msgf("Failed to unmarshal event: %v", err)
		return nil, err
	}

	events, err := svc.MatchEvents(matchingRequest)

	if err != nil {
		logger.Error().Msg(err.Error())
		return nil, err
	}

	return events, nil
}

func main() {
	_, ok := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if ok {
		lambda.Start(handleRequest)
	} else {
		request := service.MatchingRequest{
			StartDate:   time.Date(2025, 8, 31, 0, 0, 0, 0, time.UTC),
			EndDate:     time.Date(2025, 9, 30, 0, 0, 0, 0, time.UTC),
			Category:    "music",
			Description: "I want to go to a punk or indie concert preferably at a small venue",
			Venues:      []string{"The Landsdowne Hotel", "Oxford art factory"},
		}
		data, _ := json.Marshal(request)
		events, err := handleRequest(nil, data)

		if err != nil {
			log.Printf(err.Error())
			return
		}

		for _, r := range events {
			log.Printf("%+v", r)
		}
	}
}
