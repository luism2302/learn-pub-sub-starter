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
	AMQPChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Couldn't create AMQP channel. Error: %v", err)
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, "transient", handlerPause(gameState)); err != nil {
		log.Fatal(err)
	}
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), "transient", handlerMove(gameState)); err != nil {
		log.Fatal(err)
	}
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
			if err := pubsub.PublishJSON(AMQPChannel, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), move); err != nil {
				log.Printf("Couln't publish message. Error: %s", err)
				continue
			}
			log.Print("Move succesfully published!")
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.ACK
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		outcome := gs.HandleMove(move)
		fmt.Print("> ")
		var ack pubsub.AckType
		if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			ack = pubsub.ACK
		} else if outcome == gamelogic.MoveOutcomeSamePlayer {
			ack = pubsub.NACKDISCARD
		} else {
			ack = pubsub.NACKDISCARD
		}
		return ack
	}
}
