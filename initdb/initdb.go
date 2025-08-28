package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dbschema"
	"github.com/google/uuid"
	"log"
	"time"
)

const (
	defaultRegion = "us-east-1"
)

func main() {
	var region string
	var endpointURL string
	var createOnly bool

	flag.StringVar(&region, "region", defaultRegion, "AWS region (overridden by AWS_REGION env if set)")
	flag.StringVar(&endpointURL, "endpoint", "", "Custom DynamoDB endpoint (e.g., http://localhost:8000 for LocalStack)")
	flag.BoolVar(&createOnly, "create-only", false, "Only create the table; skip sample put")
	flag.Parse()

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

	dc := dynamodb.NewFromConfig(cfg)

	// Create (or ensure) tables
	if err := createEventsTable(ctx, dc); err != nil {
		log.Fatalf("createEventsTable failed: %v", err)
	}
	log.Printf("Events Table is ready")

	if err := createRawEventsTable(ctx, dc); err != nil {
		log.Fatalf("createRawEventsTable failed: %v", err)
	}
	log.Printf("RawEvents Table is ready")

	if err := createUsersTable(ctx, dc); err != nil {
		log.Fatalf("createUsersTable failed: %v", err)
	}
	log.Printf("Users Table is ready")

	if err := createSourcesTable(ctx, dc); err != nil {
		log.Fatalf("createSourcesTable failed: %v", err)
	}
	log.Printf("Sources Table is ready")

	if !createOnly {
		if err := putSampleItem(ctx, dc); err != nil {
			log.Fatalf("putSampleItem failed: %v", err)
		}
		log.Printf("Sample item inserted.")
	}

	log.Println("Done.")
}

func createEventsTable(ctx context.Context, dc *dynamodb.Client) error {
	const (
		tableName      = "Events"
		gsiSourceEvent = "SourceEvent"
		gsiStartTime   = "StartTimeIndex"
	)

	// Check if table exists
	_, err := dc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		log.Printf("Table %q already exists. Skipping creation.", tableName)
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
			{AttributeName: aws.String("source"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("source_event_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("start"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("event_id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiSourceEvent),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("source"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("source_event_id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(gsiStartTime),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("start"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest, // on-demand: no capacity planning
	}

	log.Printf("Creating table %q ...", tableName)
	if _, err := dc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(dc)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func createRawEventsTable(ctx context.Context, dc *dynamodb.Client) error {
	const (
		tableName    = "RawEvents"
		gsiFetchedAt = "FetchedAtIndex"
	)

	// Check if table exists
	_, err := dc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		log.Printf("Table %q already exists. Skipping creation.", tableName)
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

	log.Printf("Creating table %q ...", tableName)
	if _, err := dc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(dc)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func createUsersTable(ctx context.Context, dc *dynamodb.Client) error {
	const (
		tableName = "Users"
	)

	// Check if table exists
	_, err := dc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		log.Printf("Table %q already exists. Skipping creation.", tableName)
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

	log.Printf("Creating table %q ...", tableName)
	if _, err := dc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(dc)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func createSourcesTable(ctx context.Context, dc *dynamodb.Client) error {
	const (
		tableName = "Sources"
	)

	// Check if table exists
	_, err := dc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		log.Printf("Table %q already exists. Skipping creation.", tableName)
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

	log.Printf("Creating table %q ...", tableName)
	if _, err := dc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("CreateTable: %w", err)
	}

	// Wait for ACTIVE
	waiter := dynamodb.NewTableExistsWaiter(dc)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for table ACTIVE: %w", err)
	}

	return nil
}

func putSampleItem(ctx context.Context, dc *dynamodb.Client) error {
	now := time.Now().UTC()
	ev := dbschema.Event{
		EventID:     uuid.NewString(),
		Source:      "example_source",
		SourceEvent: "SRC-123456",
		Title:       "Sample Event",
		Description: "A demo event showing all fields.",
		Start:       now.Add(48 * time.Hour),
		End:         now.Add(50 * time.Hour),
		VenueName:   "Cozy Small Venue",
		Address:     dbschema.Address{Line1: "123 Example St, Sydney NSW"},
		Geo:         dbschema.Geo{Lat: -33.907, Lng: 151.159},
		URL:         "https://example.com/event",
		TicketURL:   "https://example.com/tickets",
		PriceMin:    15.00,
		PriceMax:    45.00,
		Images:      []string{"https://img.example.com/1.jpg", "https://img.example.com/2.jpg"},
		Categories:  []string{"music", "live"},
		Tags:        []string{"indie", "small-venue"},
		ContentFlags: dbschema.ContentFlags{
			SexPositive:  false,
			EighteenPlus: false,
		},
		FetchedAt: now,
	}

	av, err := attributevalue.MarshalMap(ev)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = dc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Events"),
		Item:      av,
	})
	return err
}
