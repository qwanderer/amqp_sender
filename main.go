package main

import (
	"log"
	"os"
	"flag"
	"bufio"
	"strings"
	"encoding/json"

	"github.com/vmihailenco/msgpack"
	"github.com/streadway/amqp"
)

var login, password, host, port, exchanger, queue, path_to_dump, msg_format string

func main(){
	
	flag.StringVar(&login, "login", "", "Login")
	flag.StringVar(&password, "password", "", "Password")
	flag.StringVar(&host, "host", "", "Host")
	flag.StringVar(&port, "port", "5672", "Port")
	flag.StringVar(&msg_format, "msg_format", "msgpack", "Msgpack")
	flag.StringVar(&exchanger, "exchanger", "", "Exchanger")
	flag.StringVar(&queue, "queue", "", "Queue")
	flag.StringVar(&path_to_dump, "path_to_dump", "", "Path_to_dump")
	flag.Parse()

	if login=="" {
		dd("Login param is empty")
	}

	if password=="" {
		dd("Password param is empty")
	}

	if host=="" {
		dd("Host param is empty")
	}

	if exchanger=="" {
		dd("Exchanger param is empty")
	}

	if queue=="" {
		dd("Queue param is empty")
	}

	if path_to_dump=="" {
		dd("Path_to_dump param is empty")
	}

    
	d("Start")
	connection, connection_err := amqp.Dial(strings.Join([]string{"amqp://", login, ":", password, "@", host, ":", port}, ""))
	failOnError(connection_err, "Failed to connect to RabbitMQ")
	defer connection.Close()

	d("select channel")
	channel, channel_err := connection.Channel()
	failOnError(channel_err, "Failed to open a channel")
	defer channel.Close()

	file, file_err := os.Open(path_to_dump)
	failOnError(file_err, strings.Join([]string{"Failed to open ", path_to_dump},""))
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan(){
		msgpack := getMsgPack(scanner.Text())
		send_err := sendToRabbit(channel, msgpack)
		failOnError(send_err, "Failed to publish a message")
		d("Sent")
	} // for
	d("End")
} // main


func sendToRabbit(channel *amqp.Channel, msgpack []byte) error{
	err := channel.Publish(
		exchanger,
		queue,
		false,
		false,
		amqp.Publishing{
			Body: []byte(msgpack),
		}) // publish
	return err
} // func


func getMsgPack(row string) []byte {
	in := strings.Split(row,"\t")
	var b []byte
	var err error
	if msg_format == "json" {
		b, err = json.Marshal(in)
	}else{
		b, err = msgpack.Marshal(in)
	} // if
	failOnError(err, strings.Join([]string{"Failed to convert msg to msgPack. Format: ", msg_format},""))
	return b
} // func

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func d(text string){
	log.Println(text)
}

func dd(text string){
	log.Fatal(text)
}