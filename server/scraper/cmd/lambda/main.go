package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"scraper/internal/service"
	"time"
)

type Command struct {
	Name  string `json:"name"` // can be scrape, purge, tag, createTables
	Venue string `json:"venue"`
}

type Config struct {
	Region   string `envconfig:"AWS_REGION"`
	Database struct {
		Endpoint string `envconfig:"DYNAMODB_ENDPOINT"`
	} `yaml:"database"`
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func readEnv(cfg *Config) {
	err := envconfig.Process("", cfg)
	if err != nil {
		processError(err)
	}
}

func handleRequest(ctx context.Context, request json.RawMessage) error {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339, NoColor: true})
	var cfg Config
	readEnv(&cfg)
	log.Printf("%+v", cfg)

	var command Command
	if err := json.Unmarshal(request, &command); err != nil {
		logger.Error().Msgf("Failed to unmarshal event: %v", err)
		return err
	}

	var svc = service.NewService(cfg.Database.Endpoint, cfg.Region)

	if command.Name == "scrape" {
		logger.Info().Msg("Starting scrape command")
		err := svc.LoadEvents(command.Venue)
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
	} else if command.Name == "purge" {
		logger.Info().Msg("Starting purge command")
		err := svc.Purge()
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
		return err
	} else if command.Name == "tag" {
		logger.Info().Msg("Starting tag command")
		err := svc.TagEvents()
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
		return err
	} else if command.Name == "createTables" {
		logger.Info().Msg("Starting create tables command")
		err := svc.CreateTables()
		if err != nil {
			logger.Fatal().Msg(err.Error())
		}
	} else {
		logger.Error().Msgf("Unknown command: %s", command.Name)
		return fmt.Errorf("unknown command: %s", command.Name)
	}

	return nil
}

func main() {
	_, ok := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if ok {
		lambda.Start(handleRequest)
	} else {
		var command string
		flag.StringVar(&command, "command", "", "Command to run: scrape, purge, tag, createTables")
		flag.Parse()

		err := handleRequest(nil, []byte(command))
		if err != nil {
			return
		}
	}
}
