#!/bin/bash

GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap cmd/lambda/main.go
zip bootstrap.zip bootstrap
aws lambda update-function-code  --function-name gnm-scraper  --zip-file fileb://bootstrap.zip --profile default


# creation
#aws --profile default lambda create-function --function-name gnm-scraper --runtime provided.al2023 --handler bootstrap --architecture arm64 --role arn:aws:iam::715793512778:role/gigsNearMeExecutor --zip-file fileb://bootstrap.zip

