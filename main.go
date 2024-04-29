package main

import (
	"CCLog/common"
	"CCLog/etcd"
	"CCLog/kafka"
	"CCLog/tailfile"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
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
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func main() {
	var configObj = new(Config)
	ip, err := common.GetOutboundIp()
	if err != nil {
		logrus.Errorf("get ip failed, err: %v", err)
		return
	}
	err = ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed, err: %v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)

	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("kafka int err: %v", err)
	}
	logrus.Info("init kafka success")

	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err: %v", err)
	}
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err: %v", err)
	}
	fmt.Println(allConf)
	// 监控etcd 变化
	go etcd.WatchConf(collectKey)

	err = tailfile.Init(allConf)
	if err != nil {
		logrus.Errorf("tail int err: %v", err)
	}
	logrus.Info("tail kafka success")

	if err != nil {
		logrus.Errorf("run err: %v", err)
	}
	select {}
}
