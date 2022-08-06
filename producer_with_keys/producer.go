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

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("Key-%d", i)
		fmt.Printf("producer with key = %s\n", key)
		err = pro.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &Topic_Name,
				Partition: kafka.PartitionAny,
			},

			Key: []byte(key),

			Value: []byte("Hello World!"),
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	pro.Flush(int(time.Second) / int(time.Millisecond))
}

// output
// $ go run producer.go
// producer with key = Key-0
// producer with key = Key-1
// producer with key = Key-2
// producer with key = Key-3
// producer with key = Key-4
// producer with key = Key-5
// producer with key = Key-6
// producer with key = Key-7
// producer with key = Key-8
// producer with key = Key-9
