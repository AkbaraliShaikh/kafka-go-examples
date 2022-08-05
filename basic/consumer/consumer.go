package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Topic_Name             string = "first-topic"
	Server_Address         string = "localhost"
	Group_Name             string = "my-group"
	Auto_Offset_Reset_Name string = "earliest"
)

func main() {
	con, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": Server_Address,
		"group.id":          Group_Name,
		"auto.offset.reset": Auto_Offset_Reset_Name,
	})
	if err != nil {
		log.Fatal(err)
	}

	con.SubscribeTopics([]string{Topic_Name}, nil)

	for {
		msg, err := con.ReadMessage(-1)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Message on %s : %s", msg.TopicPartition, string(msg.Value))
	}

	con.Close()
}
