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

func handleRequest(ctx context.Context, event json.RawMessage) (json.RawMessage, error) {
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

	matchingRequest.EndDate = service.Date{Time: matchingRequest.StartDate.Add(3 * 24 * time.Hour)}
	events, err := svc.MatchEvents(matchingRequest)

	if err != nil {
		logger.Error().Msg(err.Error())
		return nil, err
	}

	data, _ := json.Marshal(events)
	return data, nil
}

func main() {
	_, ok := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if ok {
		lambda.Start(handleRequest)
	} else {
		request := service.MatchingRequest{
			StartDate:   service.Date{Time: time.Date(2025, 12, 7, 0, 0, 0, 0, time.UTC)},
			Category:    "music",
			Description: "is there a gig of omar suleyman in sydney",
			Venues:      []string{"The Landsdowne Hotel", "Oxford art factory"},
		}
		data, _ := json.Marshal(request)
		log.Printf("%s", string(data))
		events, err := handleRequest(nil, data)

		if err != nil {
			log.Printf(err.Error())
			return
		}

		var recommendedEvents []service.RecommendedEvent
		if err := json.Unmarshal(events, &recommendedEvents); err != nil {
			log.Printf(err.Error())
		} else {
			for _, r := range recommendedEvents {
				log.Printf("%+v", r)
			}
		}
	}
}
