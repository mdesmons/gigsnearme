package util

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
)

type M map[string]any

func JSON(status int, body any) events.APIGatewayV2HTTPResponse {
	var data []byte
	if body == nil {
		data = []byte{}
	} else {
		b, _ := json.Marshal(body)
		data = b
	}
	return events.APIGatewayV2HTTPResponse{
		StatusCode: status,
		Headers: map[string]string{
			"Content-Type":              "application/json",
			"Cache-Control":             "no-store",
			"Strict-Transport-Security": "max-age=63072000; includeSubDomains; preload",
			"X-Content-Type-Options":    "nosniff",
			"X-Frame-Options":           "DENY",
			"Referrer-Policy":           "no-referrer",
			"Content-Security-Policy":   "default-src 'none'",
		},
		Body: string(data),
	}
}
