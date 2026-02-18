package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	connectionString = "amqp://guest:guest@127.0.0.1:5672/"
)

func main() {
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Couln't start connection to RabbitMQ server: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connection to RabbitMQ was successful")
	AMQPchannel, err := conn.Channel()
	pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), "durable")
	gamelogic.PrintServerHelp()
	running := true
	for running {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			log.Print("Sending pause message...")
			if err := pubsub.PublishJSON(AMQPchannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
				log.Fatal(err)
			}
			if err != nil {
				log.Fatalf("Couldn't create AMQP channel: %v", err)
			}
		case "resume":
			log.Print("Sending resume message...")
			if err := pubsub.PublishJSON(AMQPchannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false}); err != nil {
				log.Fatal(err)
			}
			if err != nil {
				log.Fatalf("Couldn't create AMQP channel: %v", err)
			}
		case "quit":
			log.Print("Quitting...")
			running = false
		default:
			log.Print("Unknown command")
		}
	}
}
