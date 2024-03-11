package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// producers
func main() {
	loggger := log.New(os.Stdout, "INFO:", log.Ldate|log.Ltime)
	topic := "orders"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":          "orderProducer",
		"acks":               "all"},
	)
	if err != nil {
		loggger.Fatalf("failed to create producer:%s\n", err)
		os.Exit(1)
	}
	devlivery_chan := make(chan kafka.Event, 10000)
	run := 1
	for run < 100 {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(kafka.PartitionAny)},
			Value:          []byte(fmt.Sprintf("Order %d", run)),
		},
			devlivery_chan,
		)

		if err != nil {
			loggger.Fatalf("failed to produce message :%s\n", err)
			os.Exit(1)
		}
		<-devlivery_chan
		// simulating creating a new order into the topic once every 3 seconds
		loggger.Printf("Order %d created - %v", run, time.Now())
		time.Sleep(time.Second * 3)
		run = run + 1
	}

}
