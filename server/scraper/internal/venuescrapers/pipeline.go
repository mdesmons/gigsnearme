package venuescrapers

import (
	"common"
	"context"
	"encoding/json"
	"fmt"
	"github.com/openai/openai-go/v2"
	"github.com/rs/zerolog"
)

type Scraper interface {
	Scrape(pipeline Pipeline) error
}

type Deduplicator struct {
	dbLayer common.Db
	logger  zerolog.Logger
}

func NewDeduplicator(dbLayer common.Db, logger zerolog.Logger) Deduplicator {
	return Deduplicator{
		dbLayer: dbLayer,
		logger:  logger,
	}
}

func (d *Deduplicator) Deduplicate(event common.Event) (common.Event, error) {
	// Check if an event with the same Source_name and SourceEvent already exists in the database
	// If it exists, return an error
	// If it doesn't exist, return the event as is
	events, err := d.dbLayer.QueryEventsBySourceAndSourceEventID(event.Source_name, event.SourceEvent)
	if len(events) > 0 {
		return event, fmt.Errorf("duplicate event found: %s - %s", event.Source_name, event.SourceEvent)
	}
	// return the event as is
	return event, err
}

type Tagger struct {
	dbLayer common.Db
	logger  zerolog.Logger
}

func NewTagger(dbLayer common.Db, logger zerolog.Logger) Tagger {
	return Tagger{
		dbLayer: dbLayer,
		logger:  logger,
	}
}

type PerEvent struct {
	Index      int      `json:"index"`    // position in the input slice
	Top5       []string `json:"top5"`     // exactly 5
	Extended   []string `json:"extended"` // up to 10
	Caption    string   `json:"caption"`
	Categories []string `json:"categories"`
}
type BatchOut struct {
	Results []PerEvent `json:"results"`
}

var batchOutSchema = common.GenerateSchema[BatchOut]()

func (obj *Tagger) Tag(events []common.Event) error {
	eventDescriptions := make([]string, len(events))

	for _, event := range events {
		eventDescriptions = append(eventDescriptions, event.Description)
	}
	prompt := fmt.Sprintf(`You are a tagging assistant.
Given an ordered list of event descriptions, produce results in the SAME order.
For each event i:
- "top5": exactly 5 hashtags, prioritised for reach+fit
- "extended": up to 10 more hashtags
- "caption": a punchy 1-liner using 2–3 top tags
- "categories": up to 3 categories from the set {music, culture, sex-positive, workshop, talk, other}

Return ONLY JSON that conforms to the provided schema.
Input events (0-based indices):
%v
`, eventDescriptions)

	ctx := context.Background()
	client := openai.NewClient()

	schemaParam := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:        "BatchHashtagResponse",
		Description: openai.String("Schema for batch hashtagging response"),
		Strict:      openai.Bool(true),
		Schema:      batchOutSchema,
	}
	chat, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: schemaParam},
		},
		// Only certain models can perform structured outputs
		Model: openai.ChatModelGPT4oMini,
	})

	if err != nil {
		panic(err.Error())
	}

	// The model responds with a JSON string, so parse it into a struct
	var batchOut BatchOut
	err = json.Unmarshal([]byte(chat.Choices[0].Message.Content), &batchOut)
	if err != nil {
		panic(err.Error())
	}

	for _, result := range batchOut.Results {
		if result.Index >= len(events) {
			obj.logger.Warn().Msgf("Skipping out-of-bounds result (event index %d)", result.Index)
			continue
		}

		event := events[result.Index]
		event.Tags = result.Top5
		event.ExtraTags = result.Extended
		event.Caption = result.Caption
		event.Categories = result.Categories

		_, err := obj.dbLayer.UpdateEventTags(event)
		if err != nil {
			obj.logger.Error().Msgf("Error writing tagged event %s - %s: %s", event.Source_name, event.SourceEvent, err.Error())
		} else {
			obj.logger.Debug().Msgf("Tagged event %s - %s with %d tags", event.Source_name, event.SourceEvent, len(event.Tags))
		}
	}

	return nil
}

type Saver struct {
	dbLayer common.Db
	logger  zerolog.Logger
}

func NewSaver(dbLayer common.Db, logger zerolog.Logger) Saver {
	return Saver{
		dbLayer: dbLayer,
		logger:  logger,
	}
}

func (obj Saver) Save(event common.Event) (common.Event, error) {
	err := obj.dbLayer.WriteEvent(event)
	if err != nil {
		return event, err
	}
	return event, nil
}

type Pipeline struct {
	deduplicator Deduplicator
	saver        Saver
	scrapers     map[common.SourceType]Scraper
	logger       zerolog.Logger
}

func NewPipeline(dbLayer common.Db, logger zerolog.Logger) Pipeline {
	return Pipeline{
		logger:       logger,
		deduplicator: NewDeduplicator(dbLayer, logger),
		saver:        NewSaver(dbLayer, logger),
		scrapers: map[common.SourceType]Scraper{
			common.FactoryTheatre: NewFactoryTheatreScraper(logger),
			common.Moshtix:        NewMoshtixScraper(logger),
			common.MetroTheatre:   NewMetroScraper(logger),
		},
	}
}

func (obj Pipeline) EventExists(source, sourceEvent string) (bool, error) {
	events, err := obj.deduplicator.dbLayer.QueryEventsBySourceAndSourceEventID(source, sourceEvent)
	if err != nil {
		return false, err
	}
	return len(events) > 0, nil
}

func (obj Pipeline) Process(event common.Event) (common.Event, error) {
	event, err := obj.deduplicator.Deduplicate(event)
	if err != nil {
		obj.logger.Info().Msgf("Deduplication: %s", err.Error())
		return event, err
	}

	event, err = obj.saver.Save(event)
	if err != nil {
		obj.logger.Info().Msgf("Tagging: %s", err.Error())
		return event, err
	}

	return event, nil
}

func (obj Pipeline) Scrape(sourceType common.SourceType) error {
	return obj.scrapers[sourceType].Scrape(obj)
}
