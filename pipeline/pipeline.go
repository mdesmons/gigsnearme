package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dbschema"
	"github.com/invopop/jsonschema"
	"github.com/openai/openai-go/v2"
	"log"
)

type Operation func(event dbschema.Event) (dbschema.Event, error)

type Deduplicator struct {
	dbContext context.Context
	dbClient  *dynamodb.Client
}

func NewDeduplicator(dbContext context.Context, dbClient *dynamodb.Client) Deduplicator {
	return Deduplicator{
		dbContext: dbContext,
		dbClient:  dbClient,
	}
}

func (d *Deduplicator) Deduplicate(event dbschema.Event) (dbschema.Event, error) {
	// Check if an event with the same Source_name and SourceEvent already exists in the database
	// If it exists, return an error
	// If it doesn't exist, return the event as is
	events, err := dbschema.QueryEventsBySourceAndSourceEventID(d.dbContext, d.dbClient, event.Source_name, event.SourceEvent)
	if len(events) > 0 {
		return event, fmt.Errorf("duplicate event found: %s - %s", event.Source_name, event.SourceEvent)
	}
	// return the event as is
	return event, err
}

type Tagger struct {
	dbContext context.Context
	dbClient  *dynamodb.Client
}

func NewTagger(dbContext context.Context, dbClient *dynamodb.Client) Tagger {
	return Tagger{
		dbContext: dbContext,
		dbClient:  dbClient,
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

func GenerateSchema[T any]() interface{} {
	// Structured Outputs uses a subset of JSON schema
	// These flags are necessary to comply with the subset
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}
	var v T
	schema := reflector.Reflect(v)
	return schema
}

var batchOutSchema = GenerateSchema[BatchOut]()

func (t *Tagger) Tag(events []dbschema.Event) error {
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
			log.Printf("Skipping out-of-bounds result (event index %d)", result.Index)
			continue
		}

		event := events[result.Index]
		event.Tags = result.Top5
		event.ExtraTags = result.Extended
		event.Caption = result.Caption
		event.Categories = result.Categories

		_, err := dbschema.UpdateEventTags(t.dbContext, t.dbClient, event)
		if err != nil {
			log.Printf("Error writing tagged event %s - %s: %s", event.Source_name, event.SourceEvent, err.Error())
		} else {
			log.Printf("Tagged event %s - %s with %d tags", event.Source_name, event.SourceEvent, len(event.Tags))
		}
	}

	return nil
}

type Saver struct {
	dbContext context.Context
	dbClient  *dynamodb.Client
}

func NewSaver(dbContext context.Context, dbClient *dynamodb.Client) Saver {
	return Saver{
		dbContext: dbContext,
		dbClient:  dbClient,
	}
}

func (s *Saver) Save(event dbschema.Event) (dbschema.Event, error) {
	err := dbschema.WriteEvent(s.dbContext, s.dbClient, event)
	if err != nil {
		return event, err
	}
	return event, nil
}

type Pipeline struct {
	deduplicator Deduplicator
	saver        Saver
}

func NewPipeline(dbContext context.Context, dbClient *dynamodb.Client) Pipeline {
	return Pipeline{
		deduplicator: NewDeduplicator(dbContext, dbClient),
		saver:        NewSaver(dbContext, dbClient),
	}
}

func (p *Pipeline) Process(event dbschema.Event) (dbschema.Event, error) {
	event, err := p.deduplicator.Deduplicate(event)
	if err != nil {
		log.Printf("Deduplication: %s", err.Error())
		return event, err
	}

	event, err = p.saver.Save(event)
	if err != nil {
		log.Printf("Tagging: %s", err.Error())
		return event, err
	}

	return event, nil
}
