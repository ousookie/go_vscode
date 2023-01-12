package main

import (
	"fmt"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type message struct {
	time    time.Time
	payload string
}

func printMessage(msg message) {
	fmt.Printf("Message time_stamp: %v\nMessage payload: %v\n", msg.time, msg.payload)
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-consumer-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"testTopic1"}, nil)

	for true {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			currentMessage := message{
				msg.Timestamp,
				string(msg.Value),
			}
			fmt.Printf("Incoming message\npartition [%v]\ntime [%v]\npayload [%v]\n",
				msg.TopicPartition.Partition,
				currentMessage.time,
				currentMessage.payload)
		}
	}

	c.Close()
}
