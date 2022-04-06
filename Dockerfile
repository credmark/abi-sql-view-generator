FROM golang

WORKDIR /usr/local/app
COPY . .
RUN go install
RUN go build main.go

ENTRYPOINT [ "./main" ]