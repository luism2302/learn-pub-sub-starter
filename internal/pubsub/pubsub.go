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

	queue, err := AMQPChannel.QueueDeclare(queueName, queueType == DURABLE, queueType == TRANSIENT, queueType == TRANSIENT, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't delcare queue. Error: %w", err)
	}

	if err := AMQPChannel.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't bind queue. Error: %w", err)
	}

	return AMQPChannel, queue, nil
}
