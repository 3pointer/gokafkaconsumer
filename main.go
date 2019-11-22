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

const (
	defaultWaitTimeout = 10 * time.Minute
)

type Config struct {
	addr string
	topic string
	partition int
	offset int64
	messageCount int64

	detail bool

	// ignore length of value less than `ignoreValueLen`
	ignoreValueLen int
}

func main() {
	var clientType string
	flag.StringVar(&clientType, "client", "", "which client to use: sarama, kafkago")

	c := new(Config)
	flag.StringVar(&c.addr, "addr", "localhost:9092", "")
	flag.StringVar(&c.topic, "topic", "", "")
	flag.IntVar(&c.partition, "partition", 0, "")
	flag.Int64Var(&c.offset, "offset", 0, "")
	flag.Int64Var(&c.messageCount, "max-messages", 1, "")
	flag.BoolVar(&c.detail, "detail", false, "output message content")
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
	totalValueLen := 0
	ignoreCnt := 0
	maxValueLen := 0
	for {
		select {
		case m := <- pc.Messages():
			if len(m.Value) < config.ignoreValueLen {
				ignoreCnt ++
				continue
			}
			maxCnt ++
			totalValueLen += len(m.Value)
			if config.detail {
				fmt.Println(string(m.Value))
			} else {
				fmt.Printf("sarama get message at offset: %d, valueLen: %d, keyLen: %d\n", m.Offset, len(m.Value), len(m.Key))
			}
			if maxValueLen < len(m.Value) {
				maxValueLen = len(m.Value)
			}
			if maxCnt >= config.messageCount {
				fmt.Printf("%d messages from offset %d, ignore %d message, rest messages max size is %d, total size is %d\n",
					maxCnt, config.offset, ignoreCnt, maxValueLen, totalValueLen)
				return
			}
		case m := <- pc.Errors():
			fmt.Println("met error:", m)
			time.Sleep(time.Second)
			continue
		case <-time.After(defaultWaitTimeout):
			fmt.Println("timeout")
			return
		}
	}
	pc.Close()
}

func kafkaGo(config *Config) {
	r := kafka2.NewReader(kafka2.ReaderConfig{
		Brokers:   []string{config.addr},
		Topic:     config.topic,
		Partition: config.partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 1MB
	})
	r.SetOffset(config.offset)

	ctx, cancel := context.WithTimeout(context.Background(), defaultWaitTimeout)
	defer cancel()

	maxCnt := int64(0)
	ignoreCnt := 0
	totalValueLen := 0
	maxValueLen := 0
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("met error:", err)
			break
		}
		if len(m.Value) < config.ignoreValueLen {
			ignoreCnt ++
			continue
		}
		if config.detail {
			fmt.Println(string(m.Value))
		} else {
			fmt.Printf("kafkago get message at offset %d: keyLen: %d, valueLen: %d\n", m.Offset, len(m.Key), len(m.Value))
		}
		if len(m.Value) > maxValueLen {
			maxValueLen = len(m.Value)
		}
		totalValueLen += len(m.Value)
		maxCnt ++
		if maxCnt >= config.messageCount {
			fmt.Printf("%d messages from offset %d, ignore %d message, rest messages max size is %d, total size is %d\n",
				maxCnt, config.offset, ignoreCnt, maxValueLen, totalValueLen)
			return
		}
	}
	r.Close()
}
