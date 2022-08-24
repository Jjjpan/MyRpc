package balance

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	RandomSelect int = iota
	RoundRobinSelect
	ConsistentHash
	// 其他负载均衡算法
)

/*
Discovery 服务发现接口
	包含：
	1. Refresh 从源刷新Server列表
	2. Update 更新Server列表
	3. Get 获取一个Server地址
	4. GetServers 获取全部Server地址
*/
type Discovery interface {
	Update(servers []string) error
	Get(mode int) (string, error)
	GetServers() ([]string, error)
	Refresh() error
}

/*
ServerDiscovery 服务发现
	servers 可用Server列表
	randomSeed 和 index 用于负载均衡
*/
type ServerDiscovery struct {
	randomSeed *rand.Rand
	lock       sync.RWMutex
	servers    []string
	index      int
}

func (d *ServerDiscovery) Refresh() error {
	// 暂时不在本层做实现，需要更新源
	return nil
}

// 根据servers更新Discovery.servers
func (d *ServerDiscovery) Update(servers []string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.servers = servers
	return nil
}

// 根据mode模式从服务列表中选择一个可用服务
func (d *ServerDiscovery) Get(mode int) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("请求服务信息失败，注册表空。")
	}
	switch mode {
	case RandomSelect: // 随机选择
		return d.servers[d.randomSeed.Intn(n)], nil
	case RoundRobinSelect: // 轮询
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("负载均衡模式错误。")
	}
}

// 获取整张servers表，用于容错
// TODO 容错单独模块化
func (d *ServerDiscovery) GetServers() ([]string, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	// go仅传值，需要copy深拷贝
	copy(servers, d.servers)
	return servers, nil
}

// New 一个ServerDiscovery
func NewServerDiscovery(servers []string) *ServerDiscovery {
	discover := &ServerDiscovery{
		randomSeed: rand.New(rand.NewSource(time.Now().UnixNano())),
		lock:       sync.RWMutex{},
		servers:    servers,
	}
	discover.index = discover.randomSeed.Intn(math.MaxInt32 - 1)
	return discover
}

// 判断接口满足性，无实际意义
var _ Discovery = (*ServerDiscovery)(nil)

/*
	DiscoveryFromRegistry 客户端侧的服务发现缓存，包括服务发现和心跳维护注册表
*/
type DiscoveryFromRegistry struct {
	*ServerDiscovery
	registry   string        // 注册中心地址
	timeout    time.Duration // 时限，0为10s
	lastUpdate time.Time     // 上次更新时间
}

// 默认时限--10s
const defaultTimeout = time.Second * 10

// New一个从注册中心获取更新的Discovery
func NewDiscoveryFromRegistry(registryAddress string, timeout time.Duration) *DiscoveryFromRegistry {
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &DiscoveryFromRegistry{
		ServerDiscovery: NewServerDiscovery(make([]string, 0)),
		registry:        registryAddress,
		timeout:         timeout,
	}
}

// 将clientDiscovery.servers更新为servers
func (clientDiscovery *DiscoveryFromRegistry) Update(servers []string) error {
	clientDiscovery.lock.Lock()
	defer clientDiscovery.lock.Unlock()

	clientDiscovery.servers = servers
	clientDiscovery.lastUpdate = time.Now()
	return nil
}

// Refresh 根据HTTP Get的报文头更新
// TODO：手动装包？
// 从注册中心更新servers注册表
func (clientDiscovery *DiscoveryFromRegistry) Refresh() error {
	clientDiscovery.lock.Lock()
	defer clientDiscovery.lock.Unlock()

	// 上次更新时间+时限 > 现在时间，需要更新；否则不用
	if clientDiscovery.lastUpdate.Add(clientDiscovery.timeout).After(time.Now()) {
		return nil
	}
	log.Println("从注册中心更新缓存")

	response, err := http.Get(clientDiscovery.registry)
	if err != nil {
		log.Println("注册中心更新失败: ", err)
		return err
	}

	// 同步registry/registry.go
	servers := strings.Split(response.Header.Get("Get-Servers"), ",")
	clientDiscovery.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			clientDiscovery.servers = append(clientDiscovery.servers, strings.TrimSpace(server))
		}
	}
	clientDiscovery.lastUpdate = time.Now()
	return nil
}

// 继承ServerDiscovery
func (clientDiscovery *DiscoveryFromRegistry) Get(mode int) (string, error) {
	if err := clientDiscovery.Refresh(); err != nil {
		return "", err
	}
	return clientDiscovery.ServerDiscovery.Get(mode)
}

// 继承ServerDiscovery
func (clientDiscovery *DiscoveryFromRegistry) GetAll() ([]string, error) {
	if err := clientDiscovery.Refresh(); err != nil {
		return []string{}, err
	}
	return clientDiscovery.ServerDiscovery.GetServers()
}
