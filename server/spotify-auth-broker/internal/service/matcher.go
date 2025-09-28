package service

import (
	"common"
	"context"
	"encoding/json"
	"fmt"
	"github.com/openai/openai-go/v2"
	"github.com/rs/zerolog"
)

type Matcher struct {
	dbLayer common.Db
	logger  zerolog.Logger
}

func NewMatcher(dbLayer common.Db, logger zerolog.Logger) Matcher {
	return Matcher{
		dbLayer: dbLayer,
		logger:  logger,
	}
}

type MatchingFeatures struct {
	EventId   string   `json:"event_id"`
	Tags      []string `json:"tags"`
	ExtraTags []string `json:"extra_tags"`
	VenueName string   `json:"venue_name"`
	Title     string   `json:"title"`
}

type MatchingResult struct {
	EventId     string  `json:"event_id"`
	Score       float64 `json:"score"`
	Explanation string  `json:"explanation"`
}

type MatchingResultOut struct {
	Results []MatchingResult `json:"results"`
}

var matchingResultOutSchema = common.GenerateSchema[MatchingResultOut]()

func (t *Matcher) Match(events []common.Event, description string, venues []string, artists []string) ([]common.Event, error) {
	eventFeatures := make([]MatchingFeatures, len(events))
	eventsById := make(map[string]common.Event)

	for _, event := range events {
		eventFeatures = append(eventFeatures, MatchingFeatures{
			EventId:   event.EventID,
			Tags:      event.Tags,
			ExtraTags: event.ExtraTags,
			VenueName: event.VenueName,
			Title:     event.Title,
		})
		// Create a map of events by ID for easy lookup later
		eventsById[event.EventID] = event
	}

	prompt := fmt.Sprintf(`You are a matching assistant.
Given an ordered list of event features and a user desire, preferred venues (if provided) 
and preferred artists (if provided), produce a list of at most 5 recommended events for the user, 
in order of best match first, by applying the following weights to each characteristic:
- 0.6 for matching the user desire on event tags or extra_tags
- 0.3 for matching the artist or user desire on title
- 0.1 for matching on venue_name (case insensitive substring match)

Return ONLY JSON that conforms to the provided schema, where Score is a computed score based on the above criteria,
and Explanation is a short description of how you came up with this score. 
When an event makes it to the output list, make sure you return the EventId exactly as provided in the input.

Input events:
%v

User desire: %s
User preferred venues: %v
User top artists: %v
`, eventFeatures, description, venues, artists)

	ctx := context.Background()
	client := openai.NewClient()

	schemaParam := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:        "MatchingResponse",
		Description: openai.String("Schema for matching response"),
		Strict:      openai.Bool(true),
		Schema:      matchingResultOutSchema,
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
		t.logger.Error().Msg(err.Error())
		return nil, err
	}

	// The model responds with a JSON string, so parse it into a struct
	var matchingResultOut MatchingResultOut
	err = json.Unmarshal([]byte(chat.Choices[0].Message.Content), &matchingResultOut)
	if err != nil {
		t.logger.Error().Msg(err.Error())
		return nil, err
	}

	var output = make([]common.Event, len(matchingResultOut.Results))
	for index, matchingResultTemp := range matchingResultOut.Results {
		t.logger.Debug().Msgf("EventId=%s Score=%.2f Explanation=%s\n",
			matchingResultTemp.EventId, matchingResultTemp.Score, matchingResultTemp.Explanation)

		if recommendedEvent, ok := eventsById[matchingResultTemp.EventId]; ok {
			output[index] = recommendedEvent
		} else {
			t.logger.Warn().Msgf("Skipping unknown EventId %s\n", matchingResultTemp.EventId)
		}
	}

	return output, nil
}
