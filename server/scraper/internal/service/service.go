package service

import (
	"common"
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"scraper/internal/venuescrapers"
	"time"
)

// Custom type wrapping time.Time
type Date struct {
	time.Time
}

type Service struct {
	dbLayer   common.Db
	dbContext context.Context
	pipeline  venuescrapers.Pipeline
	tagger    venuescrapers.Tagger
	logger    zerolog.Logger
}

func NewService(dynamoURL string, region string) *Service {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339, NoColor: true})
	var dbLayer, _ = common.NewDb(dynamoURL, region, logger)
	service := &Service{
		dbLayer:  dbLayer,
		pipeline: venuescrapers.NewPipeline(dbLayer, logger),
		tagger:   venuescrapers.NewTagger(dbLayer, logger),
		logger:   logger,
	}

	return service
}

func (s Service) LoadEvents(venue string) error {
	err := s.pipeline.Scrape(common.SourceType(venue))
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

func (obj Service) Purge() error {
	obj.logger.Info().Msg("Purging old events")
	return obj.dbLayer.PurgeOldEvents(time.Now())
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
