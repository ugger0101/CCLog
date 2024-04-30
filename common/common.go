package common

import (
	"fmt"
	"net"
	"strings"
)

// CollectEntry 收集的实体
type CollectEntry struct {
	Path   string `json:"path"`
	Module string `json:"module"`
	Topic  string `json:"topic"`
}

// CollectSysInfoConfig 收集系统信息的参数
type CollectSysInfoConfig struct {
	Interval int64  `json:"interval"`
	Topic    string `json:"topic"`
}

// GetOutboundIp 获取本机IP的函数
func GetOutboundIp() (ip string, err error) {
	// 通过公网去发个请求来获取
	dial, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer dial.Close()
	addr := dial.LocalAddr().(*net.UDPAddr)
	fmt.Printf(addr.String())
	ip = strings.Split(addr.IP.String(), ":")[0]
	return
}
