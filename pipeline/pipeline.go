package pipeline

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dbschema"
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
	// Implement deduplication logic here
	// For example, check if an event with the same Source and SourceEvent already exists in the database
	// If it exists, return an error or modify the event as needed
	// If it doesn't exist, return the event as is
	_, err := dbschema.QueryBySourceAndSourceEventID(d.dbContext, d.dbClient, event.Source, event.SourceEvent)
	// Placeholder implementation: return the event as is
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

func (t *Tagger) Tag(event dbschema.Event) (dbschema.Event, error) {
	// Implement tagging logic here
	// For example, add tags based on event categories or other attributes

	// Placeholder implementation: return the event as is
	return event, nil
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
	tagger       Tagger
	saver        Saver
}

func NewPipeline(dbContext context.Context, dbClient *dynamodb.Client) Pipeline {
	return Pipeline{
		deduplicator: NewDeduplicator(dbContext, dbClient),
		tagger:       NewTagger(dbContext, dbClient),
		saver:        NewSaver(dbContext, dbClient),
	}
}

func (p *Pipeline) Process(event dbschema.Event) (dbschema.Event, error) {
	event, err := p.deduplicator.Deduplicate(event)
	if err != nil {
		log.Printf("Deduplication: %s", err.Error())
		return event, err
	}

	event, err = p.tagger.Tag(event)
	if err != nil {
		log.Printf("Tagging: %s", err.Error())
		return event, err
	}

	event, err = p.saver.Save(event)
	if err != nil {
		log.Printf("Tagging: %s", err.Error())
		return event, err
	}

	return event, nil
}
