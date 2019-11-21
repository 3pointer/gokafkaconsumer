package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	kafka2 "github.com/segmentio/kafka-go"
)

type Config struct {
	addr string
	topic string
	partition int
	offset int64
	messageCount int64
}

func main() {
	var clientType string
	flag.StringVar(&clientType, "client", "", "which client to use: sarama, kafkago")

	c := new(Config)
	flag.StringVar(&c.addr, "addr", "localhost:9092", "")
	flag.StringVar(&c.topic, "topic", "", "")
	flag.IntVar(&c.partition, "partition", 0, "")
	flag.Int64Var(&c.offset, "offset", 0, "")
	flag.Int64Var(&c.messageCount, "max-message", 0, "")
	flag.Parse()

	switch strings.ToLower(clientType) {
	case "sarama":
		fmt.Println("choose sarama to consumer")
		saramaConsumer(c)
	case "kafkago":
		fmt.Println("choose kafka-go to consumer")
		kafkaGo(c)
	default:
		fmt.Println("wront client type for:", clientType, "only support sarama/kafkago")
	}
}

func saramaConsumer(config *Config) {
	sarama.MaxResponseSize = 1 << 31 - 1

	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Net.ReadTimeout = 10 * time.Minute
	// conf.Net.DialTimeout = time.Minute

	client, err := sarama.NewClient(strings.Split(config.addr, ","), conf)
	if err != nil {
		fmt.Println("new client:", err)
		return
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println("new consumer:", err)
		return
	}

	pc, err := consumer.ConsumePartition(config.topic, int32(config.partition), config.offset)
	if err != nil {
		fmt.Println("new partition consumer:", err)
		return
	}

	maxCnt := int64(0)
	for {
		select {
		case m := <- pc.Messages():
			fmt.Println(fmt.Sprintf("get message at: offset: %d, valueLen: %d, keyLen: %d", m.Offset, len(m.Value), len(m.Key)))
			maxCnt ++
			if maxCnt >= config.messageCount {
				return
			}
		case m := <- pc.Errors():
			fmt.Println("met error:", m)
			time.Sleep(time.Second)
			continue
		case <-time.After(11 * time.Minute):
			fmt.Println("timeout")
			return
		}
	}
	pc.Close()
}

func kafkaGo(config *Config) {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka2.NewReader(kafka2.ReaderConfig{
		Brokers:   []string{config.addr},
		Topic:     config.topic,
		Partition: config.partition,
		MinBytes:  10e3, // 10KBJ
		MaxBytes:  10e6, // 1GB
	})
	r.SetOffset(config.offset)

	fmt.Println("read message from", config.offset)
	maxCnt := int64(0)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("met error:", err)
			break
		}
		fmt.Printf("message at offset %d: keyLen: %d, valueLen: %d\n", m.Offset, len(m.Key), len(m.Value))
		maxCnt ++
		if maxCnt >= config.messageCount {
			return
		}
	}
	r.Close()
}
