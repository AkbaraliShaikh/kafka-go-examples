package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Topic_Name     string = "first-topic"
	Server_Address string = "localhost"
)

func main() {
	pro, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": Server_Address,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range []string{"Hello", "kafka", "I am", "Producer"} {
		err = pro.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &Topic_Name,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(msg),
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	go func() {
		for e := range pro.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Message Delivery Failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Message Delivered: %v\n", ev.TopicPartition)
				}

			}
		}
	}()

	pro.Flush(int(time.Second) / int(time.Millisecond))
}

// output
// $ go run producer.go                                                                                     ✔
// Message Delivered: first-topic[2]@22
// Message Delivered: first-topic[2]@23
// Message Delivered: first-topic[2]@24
// Message Delivered: first-topic[2]@25
