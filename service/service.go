package service

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/backend"
	"log"
	"time"
)

// TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>
type Service struct {
	dbClient  *dynamodb.Client
	dbContext context.Context
	pipeline  backend.Pipeline
	matcher   backend.Matcher
}

func NewService(dynamoURL string, region string) *Service {
	var dbClient, dbContext, _ = backend.InitDb(dynamoURL, region)
	return &Service{
		dbClient:  dbClient,
		dbContext: dbContext,
		pipeline:  backend.NewPipeline(dbContext, dbClient),
		matcher:   backend.NewMatcher(dbContext, dbClient),
	}
}

func (s *Service) LoadEvents() error {
	err := backend.Scrape(s.pipeline)
	if err != nil {
		log.Printf(err.Error())
	}
	return err
}

func (s *Service) TagEvents() error {
	tagger := backend.NewTagger(s.dbContext, s.dbClient)

	events, err := backend.QueryUntaggedEvents(s.dbContext, s.dbClient, "moshtix")
	log.Printf("Found %d untagged events\n", len(events))
	if err == nil {
		slices := len(events) / 10
		for i := 0; i <= slices; i++ {
			start := i * 10
			end := start + 10
			if end > len(events) {
				end = len(events)
			}
			batch := events[start:end]
			log.Printf("Tagging batch %d: %d events\n", i, len(batch))
			err = tagger.Tag(batch)
			if err != nil {
				log.Printf("Error tagging batch %d: %s\n", i, err.Error())
			}
		}
	}
	return err
}

func (s *Service) MatchEvents(dateFrom time.Time,
	dateTo time.Time,
	category string,
	searchString string,
	venues []string) ([]backend.Event, error) {

	events, err := backend.QueryEventsByCategoryAndDate(s.dbContext, s.dbClient,
		dateFrom, dateTo, category)

	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}

	fmt.Print("Found ", len(events), " events\n")
	eventResults, err := s.matcher.Match(events, searchString, venues)
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}

	return eventResults, nil
}
