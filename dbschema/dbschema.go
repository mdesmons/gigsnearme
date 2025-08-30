package dbschema

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

type Geo struct {
	Lat float64 `dynamodbav:"lat"`
	Lng float64 `dynamodbav:"lng"`
}

type ContentFlags struct {
	SexPositive  bool `dynamodbav:"sex_positive"`
	EighteenPlus bool `dynamodbav:"eighteen_plus"`
}

type Address struct {
	Line1    string
	Line2    string
	PostCode string
	Locality string
	Region   string
	Country  string
}

type Event struct {
	EventID      string       `dynamodbav:"event_id"`        // UUID string
	Source_name  string       `dynamodbav:"source_name"`     // GSI PK
	SourceEvent  string       `dynamodbav:"source_event_id"` // GSI SK
	Title        string       `dynamodbav:"title"`
	Description  string       `dynamodbav:"description"`
	Caption      string       `dynamodbav:"caption"`
	Start        time.Time    `dynamodbav:"start"` // stored as RFC3339 string
	End          time.Time    `dynamodbav:"end"`   // stored as RFC3339 string
	VenueName    string       `dynamodbav:"venue_name"`
	Address      Address      `dynamodbav:"address"`
	Geo          Geo          `dynamodbav:"geo"`
	URL          string       `dynamodbav:"url"`
	TicketURL    string       `dynamodbav:"ticket_url"`
	PriceMin     float64      `dynamodbav:"price_min"`
	PriceMax     float64      `dynamodbav:"price_max"`
	Images       []string     `dynamodbav:"images"`        // list of strings
	Categories   []string     `dynamodbav:"categories"`    // list of strings
	Tags         []string     `dynamodbav:"tags"`          // list of strings
	ExtraTags    []string     `dynamodbav:"extra_tag"`     // list of strings
	ContentFlags ContentFlags `dynamodbav:"content_flags"` // map of booleans
	FetchedAt    time.Time    `dynamodbav:"fetched_at"`
}

type Weight struct {
	Category string  `dynamodbav:"category"`
	Weight   float64 `dynamodbav:"weight"`
}

//////// User /////////

type Constraint struct {
	FromDate time.Time `dynamodbav:"from_date"`
	ToDate   time.Time `dynamodbav:"to_date"`
	MaxPrice float64   `dynamodbav:"max_price"`
	WeekDays bool      `dynamodbav:"week_days"`
	Radius   float64   `dynamodbav:"radius"` // in km
}

type VenueAffinity struct {
	VenueName string  `dynamodbav:"venue_name"`
	Weight    float64 `dynamodbav:"affinity"`
}

type User struct {
	UserID        string          `dynamodbav:"user_id"`        // UUID string
	City          string          `dynamodbav:"city"`           // UUID string
	Weights       []Weight        `dynamodbav:"weight"`         // UUID string
	Constraints   []Constraint    `dynamodbav:"constraints"`    // UUID string
	VenueAffinity []VenueAffinity `dynamodbav:"venue_affinity"` // UUID string
}

//////// Sources /////////

type SourceType string

const (
	Moshtix    SourceType = "moshtix"
	Eventbrite SourceType = "eventbrite"
	Humanitix  SourceType = "humanitix"
)

type Source struct {
	SourceID   string     `dynamodbav:"source_id"`   // UUID string
	Name       string     `dynamodbav:"name"`        // UUID string
	SourceType SourceType `dynamodbav:"source_type"` // UUID string
	URL        string     `dynamodbav:"url"`         // UUID string
	City       string     `dynamodbav:"city"`        // UUID string
	Tags       []string   `dynamodbav:"tags"`        // UUID string
	Active     bool       `dynamodbav:"active"`      // UUID string
}

///////// Raw Events /////////

type RawEvent struct {
	SourceID  string                 `dynamodbav:"source_id"`
	FetchedAt time.Time              `dynamodbav:"fetched_at"` // RFC3339 as string
	Payload   map[string]interface{} `dynamodbav:"payload"`    // opaque JSON
}

func WriteEvent(ctx context.Context, dc *dynamodb.Client, event Event) error {

	av, err := attributevalue.MarshalMap(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = dc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Events"),
		Item:      av,
	})
	return err
}

func InitDb(endpointURL string, region string) (*dynamodb.Client, context.Context, error) {
	ctx := context.Background()

	// Load config (honors AWS_REGION, AWS_PROFILE, etc.)
	cfg, err := config.LoadDefaultConfig(ctx, func(o *config.LoadOptions) error {
		if region != "" {
			o.Region = region
		}
		return nil
	})
	if err != nil {
		log.Fatalf("failed loading AWS config: %v", err)
	}

	if endpointURL != "" {
		// For LocalStack or custom endpoints
		cfg.BaseEndpoint = aws.String(endpointURL)
	}

	client := dynamodb.NewFromConfig(cfg)

	return client, ctx, nil
}

// Query exactly one (or few) item(s) using both GSI keys: source AND source_event_id
func QueryBySourceAndSourceEventID(ctx context.Context, ddb *dynamodb.Client, source, sourceEventID string) ([]Event, error) {
	out, err := ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Events"),
		IndexName:              aws.String("SourceEvent"),
		KeyConditionExpression: aws.String("source_name = :src AND source_event_id = :seid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":src":  &types.AttributeValueMemberS{Value: source},
			":seid": &types.AttributeValueMemberS{Value: sourceEventID},
		},
		// ConsistentRead is not supported on GSIs; default (false) is fine
		Limit: aws.Int32(10),
	})
	if err != nil {
		return nil, err
	}
	var items []Event
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func QueryBySourceWithEmptyTag(ctx context.Context, ddb *dynamodb.Client, source string) ([]Event, error) {
	out, err := ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Events"),
		IndexName:              aws.String("SourceEvent"),
		KeyConditionExpression: aws.String("source_name = :src"),
		FilterExpression:       aws.String("attribute_not_exists(tags) OR attribute_type(tags, :null) OR size(tags) = :zero"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":src":  &types.AttributeValueMemberS{Value: source},
			":zero": &types.AttributeValueMemberN{Value: "0"},
			":null": &types.AttributeValueMemberS{Value: "NULL"},
		},
		Limit: aws.Int32(100)})

	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	var items []Event
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &items); err != nil {
		return nil, err
	}
	return items, nil
}
