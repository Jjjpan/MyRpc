package client

import (
	"balance"
	"context"
	"io"
	"log"
	"reflect"
	"server"
	"sync"
)

/*
BalancedClient 包含多个client.go 中的client对象，每一个client建立一条连接，以hashMap缓存形式存在clientsMap。
	discovery: 负载均衡模块的服务发现
	mode: 负载均衡模式
	TODO 参照Dubbo，容错机制应当由Client选择
*/
type BalancedClient struct {
	discovery  balance.Discovery
	mode       int
	opt        *server.Option
	lock       sync.Mutex
	clientsMap map[string]*Client
}

func (bc *BalancedClient) Close() error {
	bc.lock.Lock()
	defer bc.lock.Unlock()
	for key, client := range bc.clientsMap {
		_ = client.Close()
		delete(bc.clientsMap, key)
	}
	return nil
}

// 同Client
var _ io.Closer = (*BalancedClient)(nil)

func NewBalancedClient(d balance.Discovery, mode int, opt *server.Option) *BalancedClient {
	return &BalancedClient{
		discovery:  d,
		mode:       mode,
		opt:        opt,
		clientsMap: make(map[string]*Client)}
}

// 检查当前缓存记录表，如果没有，则Dail一个新client并返回。
func (bc *BalancedClient) dial(rpcAddress string) (*Client, error) {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	// 查缓存
	client, ok := bc.clientsMap[rpcAddress]
	if ok && !client.isAvailable() {
		_ = client.Close()
		delete(bc.clientsMap, rpcAddress)
		client = nil
	}
	if client == nil {
		var err error
		client, err = MixDial(rpcAddress, bc.opt)
		if err != nil {
			return nil, err
		}
		bc.clientsMap[rpcAddress] = client
	}
	return client, nil
}

// CallWithTimeout的bc层封装
func (bc *BalancedClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := bc.dial(rpcAddr)
	//log.Println("dial over")
	if err != nil {
		return err
	}
	return client.CallWithTimeout(ctx, serviceMethod, args, reply)
}

// Call 先通过服务发现获取rpc地址，然后调用BalancedClient.call
func (bc *BalancedClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := bc.discovery.Get(bc.mode)
	if err != nil {
		return err
	}
	return bc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// 将请求广播给所有的服务器，等待一个返回（参考Dubbo中的容错）
// 采用Select + context实现
func (bc *BalancedClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) (res error) {
	servers, err := bc.discovery.GetServers()

	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mutex sync.Mutex

	// reply是否存在
	replyExist := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddress := range servers {
		// wg计数
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var reflectedReply interface{}
			if reply != nil {
				reflectedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := bc.call(rpcAddr, ctx, serviceMethod, args, reflectedReply)
			// err和res 在临界区
			mutex.Lock()
			// 某个call失败，取消所有协同的goroutine
			if err != nil && res == nil {
				res = err
				log.Println("try to cancel")
				cancel()
			}

			if err == nil && !replyExist {
				// .Elem()不能少
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reflectedReply).Elem())
				replyExist = true

			}
			mutex.Unlock()
		}(rpcAddress)
	}
	// 阻塞等待，存在两种情况
	// 所有goroutine均返回error，此时失败
	// 有一个goroutine返回reply，此goroutine cancel其他同ctx的goroutine，阻塞状态消失。
	wg.Wait()
	return res
}
