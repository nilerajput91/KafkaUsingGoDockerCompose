package main

import (
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	logger := log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
	topic := "orders"

	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": "localhost:29092",
			"group.id":          "stocks",
			"auto.offset.reset": "smallest",
		},
	)

	if err != nil {
		logger.Fatalf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		logger.Fatalf("Failed to subscribe: %s\n", err)
		os.Exit(1)
	}

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			logger.Printf("Processing order '%s' stock - %v", string(e.Value), time.Now())
		case kafka.Error:
			logger.Fatalf("%% Error: %v\n", e)
		}
	}

}
