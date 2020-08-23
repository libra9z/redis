package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"reflect"
)

type Subscriber struct {
	pubsub   *redis.PubSub
	channel  string
	callback processFunc
}

type processFunc func(string, string)

func NewSubscriber(channel string, fn processFunc) (*Subscriber, error) {
	var err error
	ctx := context.Background()
	s := Subscriber{
		pubsub:   client.Subscribe(ctx,channel),
		channel:  channel,
		callback: fn,
	}

	// Subscribe to the channel
	err = s.subscribe(ctx)
	if err != nil {
		return nil, err
	}

	// Listen for messages
	go s.listen()

	return &s, nil
}

func (s *Subscriber) subscribe(ctx context.Context) error {
	var err error

	err = s.pubsub.Subscribe(ctx,s.channel)
	if err != nil {
		fmt.Println("Error subscribing to channel.")
		return err
	}
	return nil
}

func (s *Subscriber) listen() error {
	var channel string
	var payload string

	for {
		//msg, err := s.pubsub.ReceiveTimeout(time.Second)
		ctx := context.Background()
		msg, err := s.pubsub.Receive(ctx)
		if err != nil {
			if reflect.TypeOf(err) == reflect.TypeOf(&net.OpError{}) && reflect.TypeOf(err.(*net.OpError).Err).String() == "*net.timeoutError" {
				// Timeout, ignore
				continue
			}
			// Actual error
			//fmt.Printf("Error in ReceiveTimeout(): %v\n", err)
		}

		channel = ""
		payload = ""

		switch m := msg.(type) {
		case *redis.Subscription:
			fmt.Printf("Subscription Message: %v to channel '%v'. %v total subscriptions.\n", m.Kind, m.Channel, m.Count)
			continue
		case *redis.Message:
			channel = m.Channel
			payload = m.Payload
		case *redis.Pong:
			payload = m.Payload
		}

		// Process the message
		go s.callback(channel, payload)
	}
}
