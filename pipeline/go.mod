module pipeline

go 1.25.0

replace github.com/dbschema => ../dbschema

require github.com/dbschema v0.0.0-00010101000000-000000000000

require (
	github.com/aws/aws-sdk-go-v2 v1.38.2 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.31.4 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.8.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.49.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.30.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.28.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.34.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.38.1 // indirect
	github.com/aws/smithy-go v1.23.0 // indirect
)
