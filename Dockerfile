# build stage
FROM golang:alpine as builder

RUN apk --no-cache add build-base git gcc

WORKDIR /usr/local/app
COPY . .
RUN go install
RUN GOARCH=amd64 GOOS=linux go build -ldflags="-s -w" -o sqlgenerator main.go

# final stage
FROM alpine

COPY --from=builder /usr/local/app/sqlgenerator .
COPY sql ./sql
COPY templates ./templates

ENTRYPOINT [ "./sqlgenerator" ]
