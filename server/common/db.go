package common

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/invopop/jsonschema"
	"github.com/rs/zerolog"
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
	Moshtix        SourceType = "moshtix"
	MetroTheatre   SourceType = "metrotheatre"
	FactoryTheatre SourceType = "factorytheatre"
	Eventbrite     SourceType = "eventbrite"
	OurSecretSpot  SourceType = "oursecretspot"
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

type Db struct {
	dbContext context.Context
	dbClient  *dynamodb.Client
	logger    zerolog.Logger
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

func NewDb(endpointURL string, region string, logger zerolog.Logger) (Db, error) {
	ctx := context.Background()

	// Load config (honors AWS_REGION, AWS_PROFILE, etc.)
	cfg, err := config.LoadDefaultConfig(ctx, func(o *config.LoadOptions) error {
		if region != "" {
			o.Region = region
		}
		return nil
	})
	if err != nil {
		logger.Fatal().Msgf("failed loading AWS config: %v", err)
	}

	if endpointURL != "" {
		// For LocalStack or custom endpoints
		cfg.BaseEndpoint = aws.String(endpointURL)
	}

	client := dynamodb.NewFromConfig(cfg)

	return Db{ctx, client, logger}, nil
}

func utcMonthBucket(t time.Time) string { return t.UTC().Format("2006-01") }

func (obj Db) WriteEvent(event Event) error {

	event.StartBucket = utcMonthBucket(event.Start)

	av, err := attributevalue.MarshalMap(event)
	if err != nil {
		obj.logger.Error().Msgf("marshal: %s", err.Error())
		return err
	}

	_, err = obj.dbClient.PutItem(obj.dbContext, &dynamodb.PutItemInput{
		TableName: aws.String("Events"),
		Item:      av,
	})
	return err
}

// Query exactly one (or few) item(s) using both GSI keys: source AND source_event_id
func (obj Db) QueryEventsBySourceAndSourceEventID(source, sourceEventID string) ([]Event, error) {
	out, err := obj.dbClient.Query(obj.dbContext, &dynamodb.QueryInput{
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

func (obj Db) QueryUntaggedEvents(source string) ([]Event, error) {

	var all []Event
	var eks map[string]types.AttributeValue
	for {

		out, err := obj.dbClient.Query(obj.dbContext, &dynamodb.QueryInput{
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
			obj.logger.Error().Msgf("marshal: %s", err.Error())
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

func (obj Db) QueryEventsByCategoryAndDate(dateFrom time.Time, dateTo time.Time, userCategory string) ([]Event, error) {

	obj.logger.Info().Msgf("Querying events between %s and %s for category %s", dateFrom, dateTo, userCategory)
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
		obj.logger.Debug().Msgf("Querying bucket %s", b)
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

			out, err := obj.dbClient.Query(obj.dbContext, &queryInput)
			if err != nil {
				obj.logger.Error().Msgf(err.Error())
				return nil, err
			}
			obj.logger.Debug().Msgf("Found %d items in this page", len(out.Items))

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

func (obj Db) UpdateEventTags(event Event) (Event, error) {
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

	response, err := obj.dbClient.UpdateItem(obj.dbContext, &dynamodb.UpdateItemInput{
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
		obj.logger.Error().Msgf("Couldn't update event %v: %v\n", event.EventID, err)
		return event, err
	} else {
		var updated Event
		err = attributevalue.UnmarshalMap(response.Attributes, &updated)
		if err != nil {
			obj.logger.Error().Msgf("Couldn't unmarshall update response: %v\n", err)
		}
		return updated, err
	}
}

func (obj Db) WriteUser(user User) error {
	av, err := attributevalue.MarshalMap(user)
	if err != nil {
		obj.logger.Error().Msgf(err.Error())
		return err
	}

	_, err = obj.dbClient.PutItem(obj.dbContext, &dynamodb.PutItemInput{
		TableName: aws.String("Users"),
		Item:      av,
	})
	return err
}

func (obj Db) QueryUserByUserID(userID string) (*User, error) {
	out, err := obj.dbClient.GetItem(obj.dbContext, &dynamodb.GetItemInput{
		TableName: aws.String("Users"),
		Key: map[string]types.AttributeValue{
			"user_id": &types.AttributeValueMemberS{Value: userID},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		obj.logger.Error().Msgf(err.Error())
		return nil, err
	}
	var user User
	if err := attributevalue.UnmarshalMap(out.Item, &user); err != nil {
		return nil, err
	}
	return &user, nil
}

func (obj Db) PurgeOldEvents(cutoff time.Time) error {
	// Convert cutoff to string (assuming ISO8601 in DB, e.g. "2025-09-01")
	cutoffStr := cutoff.Format("2006-01-02")
	obj.logger.Info().Msgf("Purging events older than %s", cutoffStr)
	// 1. Scan with filter
	scanInput := &dynamodb.ScanInput{
		TableName:        aws.String("Events"),
		FilterExpression: aws.String("#startdate < :cutoff"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cutoff": &types.AttributeValueMemberS{Value: cutoffStr},
		},
		ExpressionAttributeNames: map[string]string{"#startdate": "start"},
	}

	var toDelete []Event
	paginator := dynamodb.NewScanPaginator(obj.dbClient, scanInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(obj.dbContext)
		if err != nil {
			obj.logger.Error().Msgf("scan failed: %s", err.Error())
			return err
		}
		var events []Event
		if err := attributevalue.UnmarshalListOfMaps(page.Items, &events); err != nil {
			obj.logger.Error().Msgf("unmarshal failed: %s", err.Error())
			return err
		}
		toDelete = append(toDelete, events...)
	}

	obj.logger.Info().Msgf("Found %d events to delete", len(toDelete))
	// 2. Batch delete (max 25 per request)
	for i := 0; i < len(toDelete); i += 25 {
		obj.logger.Info().Msgf("Deleting batch %d", i)
		end := i + 25
		if end > len(toDelete) {
			end = len(toDelete)
		}
		writeReqs := make([]types.WriteRequest, 0, end-i)
		for _, e := range toDelete[i:end] {
			writeReqs = append(writeReqs, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"event_id": &types.AttributeValueMemberS{Value: e.EventID},
					},
				},
			})
		}
		_, err := obj.dbClient.BatchWriteItem(obj.dbContext, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				"Events": writeReqs,
			},
		})
		if err != nil {
			obj.logger.Error().Msgf("batch delete failed: %s", err.Error())
			return err
		}
	}

	return nil
}

func (obj Db) CreateEventsTable() error {
	const (
		tableName      = "Events"
		gsiSourceEvent = "SourceEvent"
		gsiStartTime   = "StartTimeIndex"
	)

	// Check if table exists
	_, err := obj.dbClient.DescribeTable(obj.dbContext, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		obj.logger.Info().Msgf("Table %q already exists. Skipping creation.", tableName)
		return nil
	}

	// Define table with:
	// - PK: event_id (S)
	// - GSI1: SourceEvent (source PK, source_event_id SK)
	// - GSI2: StartTimeIndex (start PK)  — start is stored as RFC3339 string
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("event_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("source_name"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("source_event_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("start"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("start_bucket"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("event_id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiSourceEvent),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("source_name"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("source_event_id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("StartBucketIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("start_bucket"), KeyType: types.KeyTypeHash}, // PK
					{AttributeName: aws.String("start"), KeyType: types.KeyTypeRange},       // SK
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest, // on-demand: no capacity planning
	}

	obj.logger.Info().Msgf("Creating table %q ...", tableName)
	if _, err := obj.dbClient.CreateTable(obj.dbContext, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(obj.dbClient)
	if err := waiter.Wait(obj.dbContext, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func (obj Db) CreateRawEventsTable() error {
	const (
		tableName    = "RawEvents"
		gsiFetchedAt = "FetchedAtIndex"
	)

	// Check if table exists
	_, err := obj.dbClient.DescribeTable(obj.dbContext, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		obj.logger.Info().Msgf("Table %q already exists. Skipping creation.", tableName)
		return nil
	}

	// Define table with:
	// - PK: event_id (S)
	// - GSI1: SourceEvent (source PK, source_event_id SK)
	// - GSI2: StartTimeIndex (start PK)  — start is stored as RFC3339 string
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("source_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("fetched_at"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("source_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("fetched_at"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiFetchedAt),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("fetched_at"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest, // on-demand: no capacity planning
	}

	obj.logger.Info().Msgf("Creating table %q ...", tableName)
	if _, err := obj.dbClient.CreateTable(obj.dbContext, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(obj.dbClient)
	if err := waiter.Wait(obj.dbContext, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func (obj Db) CreateUsersTable() error {
	const (
		tableName = "Users"
	)

	// Check if table exists
	_, err := obj.dbClient.DescribeTable(obj.dbContext, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		obj.logger.Info().Msgf("Table %q already exists. Skipping creation.", tableName)
		return nil
	}

	// Define table with:
	// - PK: event_id (S)
	// - GSI1: SourceEvent (source PK, source_event_id SK)
	// - GSI2: StartTimeIndex (start PK)  — start is stored as RFC3339 string
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("user_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("user_id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest, // on-demand: no capacity planning
	}

	obj.logger.Info().Msgf("Creating table %q ...", tableName)
	if _, err := obj.dbClient.CreateTable(obj.dbContext, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(obj.dbClient)
	if err := waiter.Wait(obj.dbContext, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func (obj Db) CreateSourcesTable() error {
	const (
		tableName = "Sources"
	)

	// Check if table exists
	_, err := obj.dbClient.DescribeTable(obj.dbContext, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		obj.logger.Info().Msgf("Table %q already exists. Skipping creation.", tableName)
		return nil
	}

	// Define table with:
	// - PK: event_id (S)
	// - GSI1: SourceEvent (source PK, source_event_id SK)
	// - GSI2: StartTimeIndex (start PK)  — start is stored as RFC3339 string
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("source_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("source_id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest, // on-demand: no capacity planning
	}

	obj.logger.Info().Msgf("Creating table %q ...", tableName)
	if _, err := obj.dbClient.CreateTable(obj.dbContext, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(obj.dbClient)
	if err := waiter.Wait(obj.dbContext, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}
