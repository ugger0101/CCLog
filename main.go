package main

import (
	"CCLog/common"
	"CCLog/etcd"
	"CCLog/info"
	"CCLog/kafka"
	"CCLog/tailfile"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	log *logrus.Logger
	wg  sync.WaitGroup
)

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}
type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}
type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}
type EtcdConfig struct {
	Address           string `ini:"address"`
	CollectKey        string `ini:"collect_key"`
	CollectSysInfoKey string `ini:"collect_sysinfo_key"`
}

func initLogger() {
	log = logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("init log success")
}

func run(logConfKey string, sysInfoConf *common.CollectSysInfoConfig) {
	wg.Add(2)
	go etcd.WatchConf(logConfKey)
	go info.Run(time.Duration(sysInfoConf.Interval)*time.Second, sysInfoConf.Topic)
	wg.Wait()
}
func main() {
	initLogger()
	// 初始化配置文件
	var conf Config
	err := ini.MapTo(&conf, "./conf/config.ini")
	if err != nil {
		panic(fmt.Sprintf("init config failed, err:%v", err))
	}
	// 初始化kafka
	err = kafka.Init(strings.Split(conf.KafkaConfig.Address, ","), conf.KafkaConfig.ChanSize)
	if err != nil {
		panic(fmt.Sprintf("init kafka failed, err:%v", err))
	}

	// 初始化etcd
	ip, err := common.GetOutboundIp()
	if err != nil {
		panic(fmt.Sprintf("get local ip failed, err:%v", err))
	}
	// 根据本机IP获取要收集日志的配置
	collectLogKey := fmt.Sprintf(conf.EtcdConfig.CollectSysInfoKey, ip)
	err = etcd.Init(strings.Split(conf.EtcdConfig.Address, ","), collectLogKey)
	if err != nil {
		panic(fmt.Sprintf("init etcd failed, err:%v", err))
	}
	log.Debug("init etcd success!")

	collectLogConf, err := etcd.GetConf(collectLogKey)
	if err != nil {
		panic(fmt.Sprintf("get collect conf from etcd failed, err:%v", err))
	}
	log.Debugf("%#v", collectLogConf)

	//  根据本机IP获取要收集系统信息的配置
	collectSysinfoKey := fmt.Sprintf(conf.EtcdConfig.CollectSysInfoKey, ip)
	collectSysinfoConf, err := etcd.GetSysinfoConf(collectSysinfoKey)
	if err != nil {
		panic(fmt.Sprintf("get collect sys info conf from etcd failed, err:%v", err))
	}
	if collectSysinfoConf == nil {
		collectSysinfoConf = &common.CollectSysInfoConfig{
			Interval: 5,
			Topic:    "collect_system_info",
		}
	}
	log.Debugf("%#v", collectSysinfoConf)

	// 获取一个新日志配置项的chan
	newConfChan := etcd.WatchChan()
	// 初始化tail
	err = tailfile.Init(collectLogConf, newConfChan) // 此处为修改后的Init
	if err != nil {
		panic(fmt.Sprintf("init tail failed, err:%v", err))
	}
	log.Debug("init tail success!")

	// 开始干活
	run(collectLogKey, collectSysinfoConf)
	log.Debug("logagent exit")
}
