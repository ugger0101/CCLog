package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	client  sarama.SyncProducer
	msgChan chan *Message
	log     *logrus.Logger
)

// Message 发送到kafka的message
type Message struct {
	Data  string
	Topic string
}

func init() {
	log = logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("kafka: init log success")

}
func Init(address []string, chanSize int64) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区
	config.Producer.Return.Successes = true                   // 确认

	// 链接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka: producer closed err:", err)
		return
	}
	msgChan = make(chan *Message, chanSize)
	go sendMsg()
	return

}

func sendMsg() {
	for msg := range msgChan {
		kafkaMsg := &sarama.ProducerMessage{}
		kafkaMsg.Topic = msg.Topic
		kafkaMsg.Value = sarama.StringEncoder(msg.Data)
		pid, offset, err := client.SendMessage(kafkaMsg)
		if err != nil {
			log.Warnf("kafka: send msg to kafka failed, err:", err)
		}
		logrus.Infof("kafka: send msg to kafka success, partition:%v offset:%v:", pid, offset)
	}
}

// SendLog 往msgChan发送消息的函数
func SendLog(msg *Message) (err error) {
	select {
	case msgChan <- msg:
	default:
		err = fmt.Errorf("kafka: msgChan is full")
	}
	return
}
