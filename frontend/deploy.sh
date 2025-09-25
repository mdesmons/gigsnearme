#!/bin/bash

npm run build
aws --profile default s3 sync dist/ s3://gnm-au --delete
