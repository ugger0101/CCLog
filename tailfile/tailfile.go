package tailfile

import (
	"CCLog/common"
	"CCLog/kafka"
	"context"
	"encoding/json"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	log     *logrus.Logger
	localIP string
)

type LogData struct {
	IP   string `json:"ip"`
	Data string `json:"data"`
}

type tailObj struct {
	path    string
	module  string
	topic   string
	tailObj *tail.Tail
	ctx     context.Context
	cancel  context.CancelFunc
}

func init() {
	log = logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("tail: init log success")
	var err error
	localIP, err = common.GetOutboundIp()
	if err != nil {
		log.Errorf("tail: get local IP failed, err: %v", err)
	}
}

func (t *tailObj) run() {
	logrus.Infof("tailfile: collect for path: %s is running", t.path)
	for {
		select {
		case <-t.ctx.Done():
			log.Warnf("the task for path:%s is stop...", t.path)
			t.tailObj.Cleanup()
			return // 函数返回对应的goroutine就结束了
		case line, ok := <-t.tailObj.Lines:
			if !ok {
				logrus.Errorf("tailfile: tailfile file close reopen, filename : %s \n", t.path)
				continue
			}
			data := &LogData{
				IP:   localIP,
				Data: line.Text,
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Warningf("tailfile: unmarshal tailfile.LodData failed, err:%v", err)
			}
			msg := &kafka.Message{
				Data:  string(jsonData),
				Topic: t.topic, // 先写死
			}
			err = kafka.SendLog(msg)
			if err != nil {
				log.Errorf("tailfile: send to kafka failed, err:%v\n", err)
			}

			////过滤空行
			//if len(strings.Trim(line.Text, "\r")) == 0 {
			//	continue
			//}
			//// 利用chan将同步读取改为异步读
			//// 把读出日志包装kafka里面的msg类型
			//msg := &kafka.Message{
			//	Data: string(j),
			//}
			//msg.Topic = t.topic
			//msg.Value = sarama.StringEncoder(line.Text)
			//
			//kafka.SendLog(msg)
		}
		log.Debug("tailfile: send msg to kafka success")
	}
}
func NewTailObj(path, moudle, topic string) (tObj *tailObj, err error) {
	tObj = &tailObj{
		path:   path,
		module: moudle,
		topic:  topic,
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	tObj.ctx = ctx
	tObj.cancel = cancelFunc
	err = tObj.Init()
	return
}

func (t *tailObj) Init() (err error) {
	_, err = tail.TailFile(t.path, tail.Config{
		ReOpen:    true,
		Follow:    true,
		MustExist: false,
		Poll:      true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
	})
	if err != nil {
		return err
	}
	if err != nil {
		log.Errorf("tailfile: init tail failed, err:", err)
		return
	}
	return
}
