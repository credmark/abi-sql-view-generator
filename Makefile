.PHONY: build

NAME   := credmark/sqlgenerator
TAG    := $$(git log -1 --pretty=%H)
IMAGE  := ${NAME}:${TAG}
LATEST := ${NAME}:latest

build-image:
	@echo "Building image..."
	@docker build -t ${NAME} . --platform linux/amd64

push-image:
	aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com

	docker tag ${NAME} ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${LATEST}
	docker tag ${NAME} ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE}

	docker push ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE}
	docker push ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${LATEST}

build:
	export GO111MODULE=on
	env GOARCH=amd64 GOOS=linux go build -ldflags="-s -w" -o sqlgenerator main.go
