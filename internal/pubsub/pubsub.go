package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DURABLE   = "durable"
	TRANSIENT = "transient"
)

type AckType string

const (
	ACK         = "Ack"
	NACKREQUEUE = "NackRequeue"
	NACKDISCARD = "NackDiscard"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshaled, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Couln't marshal val: %v to JSON. Error: %w", val, err)
	}

	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: marshaled}); err != nil {
		return fmt.Errorf("Couldn't publish message. Error: %w", err)
	}

	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	AMQPChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't create AMQP channel. Error: %w", err)
	}

	queue, err := AMQPChannel.QueueDeclare(queueName, queueType == DURABLE, queueType == TRANSIENT, queueType == TRANSIENT, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't delcare queue. Error: %w", err)
	}

	if err := AMQPChannel.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't bind queue. Error: %w", err)
	}

	return AMQPChannel, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveryChannel, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Couldn't get delivery channel. Error: %w", err)
	}

	go func() {
		for msg := range deliveryChannel {
			var data T
			if err := json.Unmarshal(msg.Body, &data); err != nil {
				fmt.Printf("Couldn't unmarshal JSON. Error: %s", err)
				continue
			}
			ackType := handler(data)
			switch ackType {
			case ACK:
				fmt.Print("Acknowledged")
				if err := msg.Ack(false); err != nil {
					fmt.Printf("Couldn't acknowledge message. Error: %s", err)
					continue
				}
			case NACKREQUEUE:
				fmt.Print("Nacked and Requed")
				if err := msg.Nack(false, true); err != nil {
					fmt.Printf("Couldn't acknowledge message. Error: %s", err)
				}
				continue
			case NACKDISCARD:
				fmt.Print("Nacked and Discarded")
				if err := msg.Nack(false, false); err != nil {
					fmt.Printf("Couldn't acknowledge message. Error: %s", err)
				}
				continue
			}

		}
	}()
	return nil
}
