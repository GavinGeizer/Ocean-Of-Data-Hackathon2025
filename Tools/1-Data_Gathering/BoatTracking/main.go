package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

type SubscribeMessage struct {
	APIKey        string        `json:"APIKey"`
	BoundingBoxes [][][]float64 `json:"BoundingBoxes"`
}

type PositionReport struct {
	UserID    int     `json:"UserID"`
	Latitude  float64 `json:"Latitude"`
	Longitude float64 `json:"Longitude"`
}

type Message struct {
	MessageType string `json:"MessageType"`
	Message     struct {
		PositionReport PositionReport `json:"PositionReport"`
	} `json:"Message"`
}

func connectAISStream(kafkaWriter *kafka.Writer) error {
	conn, _, err := websocket.DefaultDialer.Dial("wss://stream.aisstream.io/v0/stream", nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	subscribeMessage := SubscribeMessage{
		APIKey: "4275544f465907fb8c57875dcc52425666683d75",
		BoundingBoxes: [][][]float64{
			{{-180, -90}, {180, 90}},
		},
	}

	subscribeJSON, err := json.Marshal(subscribeMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe message: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, subscribeJSON); err != nil {
		return fmt.Errorf("failed to send subscribe message: %w", err)
	}

	for {
		_, messageJSON, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		var message Message
		if err := json.Unmarshal(messageJSON, &message); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		if message.MessageType == "PositionReport" {
			aisMessage := message.Message.PositionReport
			fmt.Printf("[%s] ShipId: %d Latitude: %f Longitude: %f\n",
				time.Now().UTC().Format(time.RFC3339),
				aisMessage.UserID,
				aisMessage.Latitude,
				aisMessage.Longitude,
			)

			// Publish to Kafka
if err := kafkaWriter.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(fmt.Sprintf("%d", aisMessage.UserID)),
					Value: messageJSON,
				},
			); err != nil {
				log.Printf("failed to write message to kafka: %v", err)
			}
		}
	}
}

func main() {
	//were going to wait 10 seconds to allow Kafka to start up properly
	time.Sleep(10 * time.Second)

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "ais-position-reports"
	}

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	log.Printf("Starting AIS stream, publishing to Kafka broker: %s, topic: %s", kafkaBroker, kafkaTopic)

	if err := connectAISStream(kafkaWriter); err != nil {
		log.Fatal(err)
	}
}
