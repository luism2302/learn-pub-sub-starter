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
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Couln't start connection to RabbitMQ server: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connection to RabbitMQ was successful")
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, "transient")
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)
	running := true

	for running {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				log.Printf("%s", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				log.Printf("%s", err)
				continue
			}
			log.Printf("%s moved its units to %s", move.Player.Username, move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Print("Spamming not allowed yet")
		case "quit":
			gamelogic.PrintQuit()
			running = false
		default:
			log.Print("Unknown command")
		}
	}
}
