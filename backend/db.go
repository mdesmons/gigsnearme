package backend

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
	Start        time.Time    `dynamodbav:"start"`        // stored as RFC3339 string
	StartBucket  string       `dynamodbav:"start_bucket"` // e.g. "2025-09"
	End          time.Time    `dynamodbav:"end"`          // stored as RFC3339 string
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
	ExtraTags    []string     `dynamodbav:"extra_tags"`    // list of strings
	ContentFlags ContentFlags `dynamodbav:"content_flags"` // map of booleans
	FetchedAt    time.Time    `dynamodbav:"fetched_at"`
	Tagged       bool         `dynamodbav:"tagged"` // whether the event has been tagged
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
	UserID        string          `dynamodbav:"user_id"`        // email
	PasswordHash  string          `dynamodbav:"password_hash"`  // hash
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

func utcMonthBucket(t time.Time) string { return t.UTC().Format("2006-01") }

func WriteEvent(ctx context.Context, dc *dynamodb.Client, event Event) error {

	event.StartBucket = utcMonthBucket(event.Start)

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

// Query exactly one (or few) item(s) using both GSI keys: source AND source_event_id
func QueryEventsBySourceAndSourceEventID(ctx context.Context, ddb *dynamodb.Client, source, sourceEventID string) ([]Event, error) {
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

func QueryUntaggedEvents(ctx context.Context, ddb *dynamodb.Client, source string) ([]Event, error) {

	var all []Event
	var eks map[string]types.AttributeValue
	for {

		out, err := ddb.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String("Events"),
			IndexName:              aws.String("SourceEvent"),
			KeyConditionExpression: aws.String("source_name = :src"),
			FilterExpression:       aws.String("attribute_not_exists(tagged) OR tagged = :false"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":src":   &types.AttributeValueMemberS{Value: source},
				":false": &types.AttributeValueMemberBOOL{Value: false},
			},
			Limit:             aws.Int32(100),
			ExclusiveStartKey: eks,
		},
		)

		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		var page []Event
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
			return nil, err
		}
		all = append(all, page...)
		if out.LastEvaluatedKey == nil {
			break
		}
		eks = out.LastEvaluatedKey
	}
	return all, nil
}

// buckets between two instants (inclusive), UTC months like "YYYY-MM"
func monthBuckets(from, to time.Time) []string {
	fromUTC, toUTC := from.UTC(), to.UTC()
	// normalize to first of month for iteration
	start := time.Date(fromUTC.Year(), fromUTC.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(toUTC.Year(), toUTC.Month(), 1, 0, 0, 0, 0, time.UTC)

	var res []string
	for cur := start; !cur.After(end); cur = cur.AddDate(0, 1, 0) {
		res = append(res, cur.Format("2006-01"))
	}
	return res
}

func QueryEventsByCategoryAndDate(ctx context.Context, ddb *dynamodb.Client,
	dateFrom time.Time, dateTo time.Time, userCategory string) ([]Event, error) {

	fmt.Println("Querying events between ", dateFrom, " and ", dateTo, " for category ", userCategory)
	if len(userCategory) == 0 {
		return nil, nil // nothing to match
	}

	if dateTo.Before(dateFrom) {
		dateFrom, dateTo = dateTo, dateFrom
	}

	// Build: contains(userCategories, :c0) OR contains(userCategories, :c1) ...
	eav := map[string]types.AttributeValue{
		":dateFrom": &types.AttributeValueMemberS{Value: dateFrom.UTC().Format(time.RFC3339)},
		":dateTo":   &types.AttributeValueMemberS{Value: dateTo.UTC().Format(time.RFC3339)},
		":category": &types.AttributeValueMemberS{Value: userCategory},
	}

	var all []Event
	for _, b := range monthBuckets(dateFrom, dateTo) {
		fmt.Println("Querying bucket ", b)
		var eks map[string]types.AttributeValue
		for {
			eav[":b"] = &types.AttributeValueMemberS{Value: b}

			queryInput := dynamodb.QueryInput{
				TableName:                 aws.String("Events"),
				IndexName:                 aws.String("StartBucketIndex"),
				KeyConditionExpression:    aws.String("start_bucket = :b AND #s BETWEEN :dateFrom AND :dateTo"),
				ExpressionAttributeNames:  map[string]string{"#s": "start"},
				FilterExpression:          aws.String("contains(categories, :category)"),
				ExpressionAttributeValues: eav,
				Limit:                     aws.Int32(100),
				ExclusiveStartKey:         eks,
				ScanIndexForward:          aws.Bool(true), // earliest first
			}

			out, err := ddb.Query(ctx, &queryInput)
			fmt.Println("Found ", len(out.Items), " items in this page")
			if err != nil {
				fmt.Println(err.Error())
				return nil, err
			}

			var page []Event
			if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
				return nil, err
			}

			all = append(all, page...)
			if out.LastEvaluatedKey == nil {
				break
			}
			eks = out.LastEvaluatedKey
		}
	}

	return all, nil
}

func UpdateEventTags(ctx context.Context, ddb *dynamodb.Client, event Event) (Event, error) {
	var tagAttributeValues = make([]types.AttributeValue, len(event.Tags))
	for i, tag := range event.Tags {
		tagAttributeValues[i] = &types.AttributeValueMemberS{Value: tag}
	}

	var extraTagAttributeValues = make([]types.AttributeValue, len(event.ExtraTags))
	for i, tag := range event.ExtraTags {
		extraTagAttributeValues[i] = &types.AttributeValueMemberS{Value: tag}
	}

	var categoriesValue = make([]types.AttributeValue, len(event.Categories))
	for i, tag := range event.Categories {
		categoriesValue[i] = &types.AttributeValueMemberS{Value: tag}
	}

	response, err := ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("Events"),
		Key: map[string]types.AttributeValue{
			"event_id": &types.AttributeValueMemberS{Value: event.EventID},
		},
		UpdateExpression: aws.String("SET caption = :caption, tags = :tags, extra_tags = :extra_tags, categories = :categories, tagged = :true"),
		/*		ExpressionAttributeNames: map[string]string{
					"caption":    event.Caption,   // avoid reserved word
					"tags":       event.Tags,      // avoid reserved word
					"extra_tags": event.ExtraTags, // avoid reserved word
				},
		*/ExpressionAttributeValues: map[string]types.AttributeValue{
			":caption": &types.AttributeValueMemberS{Value: event.Caption},
			":tags": &types.AttributeValueMemberL{
				Value: tagAttributeValues,
			},
			":extra_tags": &types.AttributeValueMemberL{
				Value: extraTagAttributeValues,
			},
			":categories": &types.AttributeValueMemberL{
				Value: categoriesValue,
			},
			":true": &types.AttributeValueMemberBOOL{Value: true},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})

	if err != nil {
		log.Printf("Couldn't update event %v: %v\n", event.EventID, err)
		return event, err
	} else {
		var updated Event
		err = attributevalue.UnmarshalMap(response.Attributes, &updated)
		if err != nil {
			log.Printf("Couldn't unmarshall update response: %v\n", err)
		}
		return updated, err
	}
}

func WriteUser(ctx context.Context, dc *dynamodb.Client, user User) error {
	av, err := attributevalue.MarshalMap(user)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = dc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Users"),
		Item:      av,
	})
	return err
}

func QueryUserByUserID(ctx context.Context, ddb *dynamodb.Client, userID string) (*User, error) {
	out, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("Users"),
		Key: map[string]types.AttributeValue{
			"user_id": &types.AttributeValueMemberS{Value: userID},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	var user User
	if err := attributevalue.UnmarshalMap(out.Item, &user); err != nil {
		return nil, err
	}
	return &user, nil
}
