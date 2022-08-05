package main

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Topic_Name               string = "first-topic"
	Bootstrap_Servers_Config string = "bootstrap.servers"
	Server_Address           string = "localhost"
)

func main() {
	pro, err := kafka.NewProducer(&kafka.ConfigMap{
		Bootstrap_Servers_Config: Server_Address,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = pro.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &Topic_Name,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("Hello World!"),
	}, nil)
	if err != nil {
		log.Fatal(err)
	}

	pro.Flush(int(time.Second) / int(time.Millisecond))
}