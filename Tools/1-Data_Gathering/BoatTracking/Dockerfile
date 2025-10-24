FROM golang:1.23.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN go build -o ais-stream main.go

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/ais-stream .

CMD ["./ais-stream"]
