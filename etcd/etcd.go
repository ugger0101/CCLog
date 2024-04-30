package etcd

import (
	"CCLog/common"
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"time"
)

var (
	log      *logrus.Logger
	client   *clientv3.Client
	confChan chan []*common.CollectEntry
)

func init() {
	log = logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("Etcd: init log ")

}
func Init(address []string, key string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.Errorf("etcd: Connect to Etcd failed, err: %v", err)
		return
	}
	confChan = make(chan []*common.CollectEntry)
	return
}

func GetSysinfoConf(key string) (conf *common.CollectSysInfoConfig, err error) {
	// get
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	get, err := client.Get(ctx, key)
	defer cancelFunc()
	if err != nil {
		log.Errorf("etcd:get system info config from etcd failed, err:%v\n", err)
		return
	}
	if len(get.Kvs) == 0 {
		log.Warnf("etcd:can't get any value by key:%s from etcd", key)
		return
	}
	keyValues := get.Kvs[0]
	err = json.Unmarshal(keyValues.Value, &conf)
	if err != nil {
		log.Errorf("etcd:unmarshal value from etcd failed, err:%v", err)
		return
	}
	log.Debugf("etcd:load conf from etcd success, conf:%#v", conf)
	return
}

// GetConf 获取etcd的配置
func GetConf(key string) (collectEntryList []*common.CollectEntry, err error) {
	// getConf
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	get, err := client.Get(ctx, key)
	defer cancelFunc()
	if err != nil {
		log.Error("etcd:Etcd get conf failed, err:%v", err)
		return
	}
	if len(get.Kvs) == 0 {
		log.Warningf("etcd:get len:0 conf from etcd")
		return
	}
	ret := get.Kvs[0]
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		log.Errorf("etcd:json unmarshal failed, err: %v", err)
		return
	}
	return
}

// WatchConf 监控etcd是否改变配置了
func WatchConf(key string) {
	for {
		watch := client.Watch(context.Background(), key)
		log.Debugf("etcd:watch return, is : %v", watch)
		for wresp := range watch {
			if err := wresp.Err(); err != nil {
				log.Warningf("etcd: watch key:%s, err:%v", key, err)
				continue
			}
			for _, ev := range wresp.Events {
				log.Debugf("etcd:Type: %s Key:%s Value:%s", ev.Type, ev.Kv.Key, ev.Kv.Value)
				// 获取最新的日志配置项通过common的实体来通过chan来传
				var newConf []*common.CollectEntry
				// 如果是删除就不用解码了，直接通知就行
				if ev.Type == clientv3.EventTypeDelete {
					confChan <- newConf
					continue
				}
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					log.Warnf("etcd:unmarshal the conf from etcd failed, err:%v", err)
					continue
				}
				confChan <- newConf
				log.Debug("etcd:send newConf to confChan success")

			}
		}
		logrus.Infof("etcd:watch new conf ")
	}
}

func WatchChan() <-chan []*common.CollectEntry {
	return confChan
}
