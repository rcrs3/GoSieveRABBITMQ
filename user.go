package main

import (
        "fmt"
        "log"
        "strconv"
        "github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

func createQueue(name string) {
        conn, err := amqp.Dial("amqp://localhost:5672/")
        failOnError(err, "Failed to connect to RabbitMQ")
        defer conn.Close()

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")
        defer ch.Close()

        q, err := ch.QueueDeclare(
                name, // name
                true,   // durable
                false,   // delete when usused
                false,   // exclusive
                false,   // no-wait
                nil,     // arguments
              )
              failOnError(err, "Failed to declare a queue")
        fmt.Println("Created queue " + q.Name)
              
}

func producer(queueName string, msg string) {
        conn, err := amqp.Dial("amqp://localhost:5672/")
        failOnError(err, "Failed to connect to RabbitMQ")
        defer conn.Close()

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")
        defer ch.Close()

        createQueue(queueName)

        cont := 1
        for {
                if cont > 200 {
                        break;
                }
                body := msg + strconv.Itoa(cont)
                err = ch.Publish(
                        "", // exchange
                        queueName,     // routing key
                        false,  // mandatory
                        false,  // immediate
                        amqp.Publishing{
                                ContentType: "text/plain",
                                Body:        []byte(body),
                        })
                failOnError(err, "Failed to publish a message")
                cont++
                log.Printf(" [x] Sent %s", body)
        }
}

func consumer(queueName string) {
        conn, err := amqp.Dial("amqp://localhost:5672/")
        failOnError(err, "Failed to connect to RabbitMQ")
        defer conn.Close()

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")
        defer ch.Close()

        msgs, err := ch.Consume(
                queueName, // queue
                "",     // consumer
                true,   // auto-ack
                false,  // exclusive
                false,  // no-local
                false,  // no-wait
                nil,    // args
        )
        failOnError(err, "Failed to register a consumer")

        forever := make(chan bool)

        go func() {
                
                for d := range msgs {
                        log.Printf(" [x] Received %s", d.Body)
                }
        }()

        <-forever
}

func main() {
        go producer("client2", "Olá")
        go producer("client1", "Oi")
        go consumer("client1")
        consumer("client2")
}
