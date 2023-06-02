package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RabiQ struct {
	options    *option
	ex_options *ex_option
	conn       *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
	confirmations chan amqp.Confirmation
	confirm       bool
   durable   bool
}

type option struct {
	url         string
	q_name      string
	q_delete    bool
	q_exclusive bool
	q_no_waite  bool
	q_arguments amqp.Table
	mandatory   bool
	immediat    bool
	r_key       string
}
type ex_option struct {
	name           string
	ex_type        string
	ex_auto_delete bool
	ex_internal    bool
	ex_no_wait     bool
	ex_arguments   amqp.Table
}

func defaultOpts() *option {
	return &option{
		q_delete:    false,
		q_exclusive: false,
		q_no_waite:  false,
		q_arguments: nil,
		mandatory:   false,
		immediat:    false,
		r_key:       "",
	}
}

var once sync.Once

func defaultExOpts() *ex_option {
	return &ex_option{
		name:           "",
		ex_type:        "fanout",
		ex_auto_delete: false,
		ex_internal:    false,
		ex_no_wait:     false,
		ex_arguments:   nil,
	}
}

type (
	OptsFunc   func(*option)
	ExOptsFunc func(*ex_option)
)

func WithUrl(url string) OptsFunc {
	return func(opts *option) {
		opts.url = url
	}
}

func WithName(name string) OptsFunc {
	return func(opts *option) {
		opts.q_name = name
	}
}

func NewRabiQ(fns ...OptsFunc) (*RabiQ, error) {
	options := defaultOpts()
	for _, fn := range fns {
		fn(options)
	}
	conn, err := amqp.Dial(options.url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabiQ{
		conn:    conn,
		channel: ch,
		options: options,
        durable: true,
	}, nil
}

func (r *RabiQ) EnableConfirmation() {
	err := r.channel.Confirm(false)
	failOnError(err, "Failed to set channel in confirm mode")
	r.confirmations = r.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	go func() {
		counter := 0
		for {
			select {
			case confirmation := <-r.confirmations:
				counter++
				if confirmation.Ack {
					log.Printf(" [x] Message confirmed: delivery tag %d", confirmation.DeliveryTag)
				} else {
					log.Fatalf(" [!] Message not confirmed: delivery tag %d, counter %d", confirmation.DeliveryTag, counter)
				}
			}
		}
	}()
}

func (r *RabiQ) SetExchange(name string, fns ...ExOptsFunc) error {
	ex_options := defaultExOpts()
	for _, fn := range fns {
		fn(ex_options)
	}
	ex_options.name = name
	r.durable = true
	err := r.channel.ExchangeDeclare(
		ex_options.name,           // Exchange name
		ex_options.ex_type,        // Exchange type
		r.durable,       // Durable
		ex_options.ex_auto_delete, // Auto-deleted
		ex_options.ex_internal,    // Internal
		ex_options.ex_no_wait,     // No-wait
		ex_options.ex_arguments,   // Arguments
	)
	failOnError(err, "Failed to declare an exchange")
	//r.options.q_name = ""
	r.ex_options = ex_options
	return nil
}

func (r *RabiQ) SendMessage(contentType, body string) error {
	once.Do(
		func() {
			if r.options.q_name != "" {
				q, err := r.channel.QueueDeclare(
					r.options.q_name,
					r.durable,
					r.options.q_delete,
					r.options.q_exclusive,
					r.options.q_no_waite,
					nil,
				)
				if err != nil {
					fmt.Println("Once ", err)
					return
				}
				r.queue = &q
				fmt.Println("Queue Name: ", q.Name)
			}
		})
	return r.channel.Publish(
		r.ex_options.name,
		r.options.q_name, //
		r.options.mandatory,
		r.options.immediat,
		amqp.Publishing{
			ContentType: contentType,
			Body:        []byte(body),
			Priority:    9,
			MessageId:   uuid.NewString(),
			Timestamp:   time.Now(),
		})
}

func (r *RabiQ) SetDurable(durable bool) {
	r.durable = durable
}

func (r *RabiQ) StartConsum() error {
	ch, err := r.conn.Channel()
	failOnError(err, "Failed to get channel ")
	ch.Qos(1, 0, false) // Set prefetch count to 1
	msgs, err := ch.Consume(
		r.options.q_name, // queue
		"",               // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	failOnError(err, "Failed to register a consumer")

	done := make(chan struct{})
	go func() {
		for d := range msgs {
			log.Printf(" [Priority] %v", d.Priority)
			log.Printf(" [MessageID] %v", d.MessageId)
			time.Sleep(4 * time.Second) //slow consumer
			d.Ack(false)
			//			d.Nack(false, true)
		}
		done <- struct{}{}
	}()
	<-done
	return nil
}

func main() {
	q, err := NewRabiQ(
		WithName("hello"),
		WithUrl("amqp://guest:guest@localhost:5672"))
	failOnError(err, "Failed to create RabiQ ")
	q.SetDurable(true)
	q.SetExchange("my_exchange")
	q.EnableConfirmation()
	go q.StartConsum()

	reader := bufio.NewScanner(os.Stdin)
	reader.Split(bufio.ScanLines)

	for reader.Scan() {
		body := reader.Text()
		fmt.Println(body)

		er := q.SendMessage("text/plain", string(body))
		if er != nil {
			fmt.Println("Send ", body)
		}
	}
	select {}
}
