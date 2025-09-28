package service

import (
	"common"
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

// Custom type wrapping time.Time
type Date struct {
	time.Time
}

// Implement UnmarshalJSON so it can parse "YYYY-MM-DD"
func (d *Date) UnmarshalJSON(b []byte) error {
	// Trim quotes
	s := string(b)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	// Parse using the right layout
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return err
	}
	d.Time = t
	return nil
}

// Format back to "YYYY-MM-DD"
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Format("2006-01-02"))
}

type MatchingRequest struct {
	// Time range
	StartDate   Date     `json:"start_date"`
	EndDate     Date     `json:"end_date"`
	Category    string   `json:"category"`
	Description string   `json:"description"`
	Venues      []string `json:"venues"`
	Artists     []string `json:"artists"`
}

type RecommendedEvent struct {
	Source_name  string // GSI PK
	Title        string
	Description  string
	Caption      string
	Start        time.Time // stored as RFC3339 string
	VenueName    string
	Address      common.Address
	Geo          common.Geo
	URL          string
	TicketURL    string
	PriceMin     float64
	PriceMax     float64
	Images       []string            // list of strings
	Categories   []string            // list of strings
	ContentFlags common.ContentFlags // map of booleans
}

type Service struct {
	dbLayer   common.Db
	dbContext context.Context
	matcher   Matcher
	logger    zerolog.Logger
}

func (s Service) ConvertEvent(event common.Event) RecommendedEvent {
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
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339, NoColor: true})
	var dbLayer, _ = common.NewDb(dynamoURL, region, logger)
	service := &Service{
		dbLayer: dbLayer,
		matcher: NewMatcher(dbLayer, logger),
		logger:  logger,
	}

	return service
}

func (s Service) MatchEvents(request MatchingRequest) ([]RecommendedEvent, error) {
	s.logger.Debug().Msgf("Matching events from %s to %s, category: %s, searchString: %s, venues: %v\n",
		request.StartDate.Format("2006-01-02"),
		request.EndDate.Format("2006-01-02"),
		request.Category,
		request.Description,
		request.Venues)

	events, err := s.dbLayer.QueryEventsByCategoryAndDate(request.StartDate.Time, request.EndDate.Time, request.Category)

	if err != nil {
		s.logger.Error().Msg(err.Error())
		return nil, err
	}

	s.logger.Debug().Msgf("Found %d events to match against", len(events))
	eventsRecommendedByMatcher, err := s.matcher.Match(events, request.Description, request.Venues, request.Artists)
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
