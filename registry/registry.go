package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultPath    = "/registry"
	defaultTimeout = time.Minute * 3
)

/*
ServerItem
	Addr	服务端地址
	start	服务开始时间（开始时间+有效时间=过期时间）
*/
type ServerItem struct {
	Addr  string
	start time.Time
}

/*
RegistryCenter
	timeout	有效时间
	lock	Mutex锁
	servers	记录在册的服务端
*/
type RegistryCenter struct {
	timeout time.Duration
	lock    sync.Mutex
	servers map[string]*ServerItem
}

// NewRegistry 创建注册中心
func NewRegistry(timeout time.Duration) *RegistryCenter {
	return &RegistryCenter{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

// 默认配置
var DefaultGeeRegistry = NewRegistry(defaultTimeout)

// 添加一个服务端
func (registry *RegistryCenter) putServer(addr string) {
	registry.lock.Lock()
	defer registry.lock.Unlock()

	s, ok := registry.servers[addr]
	if !ok {
		registry.servers[addr] = &ServerItem{
			start: time.Now(),
			Addr:  addr,
		}
	} else {
		s.start = time.Now()
	}
}

// 获得所有当前有效的服务器地址，检查有效器==0（无限）或 开始时间+有效时间 < 过期时间，对过期记录直接删除。
// TODO 参照Reddis做懒删除减轻注册中心维护压力
func (registry *RegistryCenter) getAliveServers() (res []string) {
	registry.lock.Lock()
	defer registry.lock.Unlock()

	// 检查有效器==0（无限）或 开始时间+有效时间 < 过期时间
	for k, v := range registry.servers {
		if registry.timeout == 0 || v.start.Add(registry.timeout).After(time.Now()) {
			res = append(res, k)
		} else {
			delete(registry.servers, k)
		}
	}
	sort.Strings(res)
	return res
}

// 注册中心使用HTTP1.1协议提供服务，所有信息都封装在HTTP报文头的 Get-Servers 和 Post-Server字段。
func (registry *RegistryCenter) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case "GET":
		responseWriter.Header().Set("Get-Servers", strings.Join(registry.getAliveServers(), ","))
	case "POST":
		address := request.Header.Get("Post-Server")
		if address == "" {
			responseWriter.WriteHeader(http.StatusInternalServerError)
			return
		}
		// 收到POST请求，将address更新到server表中
		registry.putServer(address)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}
func (registry *RegistryCenter) HandleHTTP(path string) {
	http.Handle(path, registry)
	log.Println("registry path: " + path)
}

// 检查接口
var _ http.Handler = (*RegistryCenter)(nil)

// 通过定时心跳来保活，服务端主动发送心跳
func sendHeartbeat(registry, address string) error {
	log.Println(address + " send heartbeat to registry " + registry)
	httpClient := http.Client{}
	request, err := http.NewRequest("POST", registry, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Post-Server", address)
	_, err = httpClient.Do(request)
	if err != nil {
		log.Println("服务端心跳错误:", err)
		return err
	}
	return nil
}

// HeartBeat 服务器定时提供心跳来刷新注册中心的对应开始时间
func HeartBeat(registry string, address string, duration time.Duration) (err error) {
	// 默认一秒一次
	if duration == 0 {
		duration = time.Second
	}
	err = sendHeartbeat(registry, address)
	go func() {
		// 无缓冲通道阻塞Ticker实现定时心跳
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
		}
	}()
	return
}
