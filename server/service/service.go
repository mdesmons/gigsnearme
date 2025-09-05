package service

import (
	"context"
	"github.com/backend"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

type RecommendedEvent struct {
	Source_name  string // GSI PK
	Title        string
	Description  string
	Caption      string
	Start        time.Time // stored as RFC3339 string
	VenueName    string
	Address      backend.Address
	Geo          backend.Geo
	URL          string
	TicketURL    string
	PriceMin     float64
	PriceMax     float64
	Images       []string             // list of strings
	Categories   []string             // list of strings
	ContentFlags backend.ContentFlags // map of booleans
}

type Service struct {
	dbLayer   backend.Db
	dbContext context.Context
	pipeline  backend.Pipeline
	matcher   backend.Matcher
	tagger    backend.Tagger
	logger    zerolog.Logger
}

func (s Service) ConvertEvent(event backend.Event) RecommendedEvent {
	return RecommendedEvent{
		Source_name:  event.Source_name,
		Title:        event.Title,
		Description:  event.Description,
		Caption:      event.Caption,
		Start:        event.Start,
		VenueName:    event.VenueName,
		Address:      event.Address,
		Geo:          event.Geo,
		URL:          event.URL,
		TicketURL:    event.TicketURL,
		PriceMin:     event.PriceMin,
		PriceMax:     event.PriceMax,
		Images:       event.Images,
		Categories:   event.Categories,
		ContentFlags: event.ContentFlags,
	}
}

func NewService(dynamoURL string, region string) *Service {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	var dbLayer, _ = backend.NewDb(dynamoURL, region, logger)
	service := &Service{
		dbLayer:  dbLayer,
		pipeline: backend.NewPipeline(dbLayer, logger),
		matcher:  backend.NewMatcher(dbLayer, logger),
		tagger:   backend.NewTagger(dbLayer, logger),
		logger:   logger,
	}

	return service
}

func (s Service) LoadEvents() error {
	err := s.pipeline.Scrape()
	if err != nil {
		s.logger.Error().Msg(err.Error())
	}

	return nil
}

func (s Service) tagEventsForSource(source string) error {
	s.logger.Info().Msg("Tagging events for source " + source)

	events, err := s.dbLayer.QueryUntaggedEvents(source)
	s.logger.Info().Msgf("Found %d untagged events", len(events))

	if err == nil {
		slices := len(events) / 10
		for i := 0; i <= slices; i++ {
			start := i * 10
			end := start + 10
			if end > len(events) {
				end = len(events)
			}
			batch := events[start:end]
			s.logger.Info().Msgf("Tagging batch %d: %d events\n", i, len(batch))
			err = s.tagger.Tag(batch)
			if err != nil {
				s.logger.Error().Msgf("Error tagging batch %d: %s\n", i, err.Error())
			}
		}
	} else {
		s.logger.Error().Msg(err.Error())
	}
	return err
}

func (s Service) TagEvents() error {
	s.tagEventsForSource("metrotheatre")
	s.tagEventsForSource("moshtix")

	return nil
}

func (s Service) MatchEvents(dateFrom time.Time,
	dateTo time.Time,
	category string,
	searchString string,
	venues []string) ([]RecommendedEvent, error) {
	s.logger.Debug().Msgf("Matching events from %s to %s, category: %s, searchString: %s, venues: %v\n",
		dateFrom.Format("2006-01-02"), dateTo.Format("2006-01-02"), category, searchString, venues)

	events, err := s.dbLayer.QueryEventsByCategoryAndDate(dateFrom, dateTo, category)

	if err != nil {
		s.logger.Error().Msg(err.Error())
		return nil, err
	}

	s.logger.Debug().Msgf("Found %d events to match against", len(events))
	eventsRecommendedByMatcher, err := s.matcher.Match(events, searchString, venues)
	if err != nil {
		s.logger.Error().Msg(err.Error())
		return nil, err
	}

	result := make([]RecommendedEvent, len(eventsRecommendedByMatcher))
	for i, event := range eventsRecommendedByMatcher {
		result[i] = s.ConvertEvent(event)
	}

	return result, nil
}

func (s Service) CreateTables() error {

	// Create (or ensure) tables
	if err := s.dbLayer.CreateEventsTable(); err != nil {
		s.logger.Fatal().Msgf("createEventsTable failed: %v", err)
	}
	s.logger.Info().Msgf("Events Table is ready")

	if err := s.dbLayer.CreateRawEventsTable(); err != nil {
		s.logger.Fatal().Msgf("createRawEventsTable failed: %v", err)
	}
	s.logger.Info().Msgf("RawEvents Table is ready")

	if err := s.dbLayer.CreateUsersTable(); err != nil {
		s.logger.Fatal().Msgf("createUsersTable failed: %v", err)
	}
	s.logger.Info().Msgf("Users Table is ready")

	if err := s.dbLayer.CreateSourcesTable(); err != nil {
		s.logger.Fatal().Msgf("createSourcesTable failed: %v", err)
	}
	s.logger.Info().Msgf("Sources Table is ready")
	return nil
}
