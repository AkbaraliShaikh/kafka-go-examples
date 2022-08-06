package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Topic_Name                string = "first-topic"
	Server_Address            string = "localhost"
	Group_Name                string = "my-group"
	Auto_Offset_Reset_Name    string = "earliest"
	Cooperative_Strategy_Name string = "cooperative-sticky"
)

func main() {
	singChan := make(chan os.Signal, 1)
	signal.Notify(singChan, syscall.SIGINT, syscall.SIGTERM)

	con, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             Server_Address,
		"group.id":                      Group_Name,
		"auto.offset.reset":             Auto_Offset_Reset_Name,
		"partition.assignment.strategy": Cooperative_Strategy_Name,
	})
	if err != nil {
		log.Fatal(err)
	}

	con.SubscribeTopics([]string{Topic_Name}, nil)

	run := true
	for run {
		select {
		case <-singChan:
			fmt.Println("Exiting...")
			run = false

		default:
			msg := con.Poll(100)
			if msg == nil {
				continue
			}

			switch e := msg.(type) {
			case *kafka.Message:
				fmt.Printf("Message on %s : %s\n", e.TopicPartition, string(e.Value))

			case kafka.Error:
				fmt.Printf("Error, %v : %v\n", e.Code(), e)
			}
		}
	}

	fmt.Println("Exited")
	con.Close()
}

// output
// $ go run consumer.go
// Message on first-topic[2]@43 : Hello World!
// Message on first-topic[2]@44 : Hello World!
// Message on first-topic[2]@45 : Hello World!
// Message on first-topic[0]@43 : Hello World!
// Message on first-topic[0]@44 : Hello World!
// Message on first-topic[1]@44 : Hello World!
// Message on first-topic[1]@45 : Hello World!
// Message on first-topic[1]@46 : Hello World!
// Message on first-topic[1]@47 : Hello World!
// Message on first-topic[1]@48 : Hello World!
// ^CExiting...
// Exited
