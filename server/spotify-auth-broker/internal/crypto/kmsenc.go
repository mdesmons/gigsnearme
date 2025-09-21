package crypto

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

type KMS struct {
	svc *kms.Client
	key string
}

func NewKMS(cfg aws.Config, keyID string) *KMS {
	return &KMS{svc: kms.NewFromConfig(cfg), key: keyID}
}

func (k *KMS) Encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	out, err := k.svc.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(k.key),
		Plaintext: pt,
	})
	if err != nil {
		return nil, err
	}
	return out.CiphertextBlob, nil
}

func (k *KMS) Decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	out, err := k.svc.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: ct,
	})
	if err != nil {
		return nil, err
	}
	return out.Plaintext, nil
}
