package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Topic_Name               string = "first-topic"
	Bootstrap_Servers_Config string = "bootstrap.servers"
	Server_Address           string = "localhost"
	Group_Id                 string = "group.id"
	Group_Name               string = "my-group"
	Auto_Offset_Reset        string = "auto.offset.reset"
	Auto_Offset_Reset_Name   string = "earliest"
)

func main() {
	con, err := kafka.NewConsumer(&kafka.ConfigMap{
		Bootstrap_Servers_Config: Server_Address,
		Group_Id:                 Group_Name,
		Auto_Offset_Reset:        Auto_Offset_Reset_Name,
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
