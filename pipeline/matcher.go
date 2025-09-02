package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dbschema"
	"github.com/openai/openai-go/v2"
)

type Matcher struct {
	dbContext context.Context
	dbClient  *dynamodb.Client
}

func NewMatcher(dbContext context.Context, dbClient *dynamodb.Client) Matcher {
	return Matcher{
		dbContext: dbContext,
		dbClient:  dbClient,
	}
}

type MatchingFeatures struct {
	EventId    string   `json:"event_id"`
	Categories []string `json:"categories"`
	Tags       []string `json:"tags"`
	ExtraTags  []string `json:"extra_tags"`
	VenueName  string   `json:"venue_name"`
}

type MatchingResult struct {
	EventId     string  `json:"event_id"`
	Score       float64 `json:"score"`
	Explanation string  `json:"explanation"`
}

type MatchingResultOut struct {
	Results []MatchingResult `json:"results"`
}

var matchingResultOutSchema = GenerateSchema[MatchingResultOut]()

func (t *Matcher) Match(events []dbschema.Event, desire string, venues []string) ([]dbschema.Event, error) {
	eventFeatures := make([]MatchingFeatures, len(events))
	eventsById := make(map[string]dbschema.Event)

	for _, event := range events {
		eventFeatures = append(eventFeatures, MatchingFeatures{
			EventId:    event.EventID,
			Categories: event.Categories,
			Tags:       event.Tags,
			ExtraTags:  event.ExtraTags,
			VenueName:  event.VenueName,
		})
		// Create a map of events by ID for easy lookup later
		eventsById[event.EventID] = event
	}

	prompt := fmt.Sprintf(`You are a matching assistant.
Given an ordered list of event features and a user desire and preferred venues (if provided), produce a list of at most 5 recommended events for the user, 
in order of best match first, by applying the following weights to each characteristic:
- 0.5 for matching the user desire on event tags or extra_tags
- 0.4 for matching event categories to user desire
- 0.1 for matching on venue_name (case insensitive substring match)

Return ONLY JSON that conforms to the provided schema, where Score is a computed score based on the above criteria,
and Explanation is a short description of how you came up with this score.
Input events:
%v

User desire: %s
User preferred venues: %v
`, eventFeatures, desire, venues)

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
		panic(err.Error())
	}

	// The model responds with a JSON string, so parse it into a struct
	var matchingResultOut MatchingResultOut
	err = json.Unmarshal([]byte(chat.Choices[0].Message.Content), &matchingResultOut)
	if err != nil {
		panic(err.Error())
	}

	var output = make([]dbschema.Event, len(matchingResultOut.Results))
	for index, matchingResultTemp := range matchingResultOut.Results {
		fmt.Printf("EventId=%s Score=%.2f Explanation=%s\n",
			matchingResultTemp.EventId, matchingResultTemp.Score, matchingResultTemp.Explanation)

		if recommendedEvent, ok := eventsById[matchingResultTemp.EventId]; ok {
			output[index] = recommendedEvent
		} else {
			fmt.Printf("Skipping unknown EventId %s\n", matchingResultTemp.EventId)
		}
	}

	return output, nil
}
