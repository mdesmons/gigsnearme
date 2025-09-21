package store

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"spotify-auth-broker/internal/crypto"
)

type DDB struct {
	table string
	ddb   *dynamodb.Client
	kms   *crypto.KMS
}

type Link struct {
	UserID       string
	RefreshToken string
	Scope        string
	UpdatedAt    time.Time
}

func MustNew() *DDB {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	return &DDB{
		table: os.Getenv("DDB_TABLE"),
		ddb:   dynamodb.NewFromConfig(cfg),
		kms:   crypto.NewKMS(cfg, os.Getenv("KMS_KEY_ID")),
	}
}

func (s *DDB) UpsertRefreshToken(ctx context.Context, userID, refresh, scope string) error {
	ct, err := s.kms.Encrypt(ctx, []byte(refresh))
	if err != nil {
		return err
	}
	enc := base64.StdEncoding.EncodeToString(ct)
	_, err = s.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.table,
		Item: map[string]ddbtypes.AttributeValue{
			"user_id":     &ddbtypes.AttributeValueMemberS{Value: userID},
			"refresh_enc": &ddbtypes.AttributeValueMemberS{Value: enc},
			"scope":       &ddbtypes.AttributeValueMemberS{Value: scope},
			"updated_at":  &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Unix(), 10)},
		},
	})
	return err
}

func (s *DDB) GetLink(ctx context.Context, userID string) (*Link, error) {
	out, err := s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table,
		Key: map[string]ddbtypes.AttributeValue{
			"user_id": &ddbtypes.AttributeValueMemberS{Value: userID},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, errors.New("not found")
	}
	enc := out.Item["refresh_enc"].(*ddbtypes.AttributeValueMemberS).Value
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, err
	}
	pt, err := s.kms.Decrypt(ctx, raw)
	if err != nil {
		return nil, err
	}
	scope := getS(out.Item["scope"])
	ts := getN(out.Item["updated_at"])
	return &Link{
		UserID:       userID,
		RefreshToken: string(pt),
		Scope:        scope,
		UpdatedAt:    time.Unix(ts, 0),
	}, nil
}

func (s *DDB) DeleteLink(ctx context.Context, userID string) error {
	_, err := s.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.table,
		Key: map[string]ddbtypes.AttributeValue{
			"user_id": &ddbtypes.AttributeValueMemberS{Value: userID},
		},
	})
	return err
}

// helpers
func getS(v ddbtypes.AttributeValue) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}
func getN(v ddbtypes.AttributeValue) int64 {
	if v == nil {
		return 0
	}
	if n, ok := v.(*ddbtypes.AttributeValueMemberN); ok {
		i, _ := strconv.ParseInt(n.Value, 10, 64)
		return i
	}
	return 0
}
