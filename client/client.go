package client

import (
	"bufio"
	"context"
	"encode"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"server"
	"strings"
	"sync"
	"time"
)

/*
Call 一次RPC调用
包含：
	SequenceNum           序列号，参考Dubbo，后续可拓展HTTP2.0的乱序发送/接受。
	ServiceMethod 服务类型
	Args          参数
	Reply         返回值
	Error         错误
	Done          完成通知chan
*/
type Call struct {
	SequenceNum   uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

// 告知已完成信道,done表示当前call完成，但并不保证已执行成功(意外结束)
func (call *Call) done() {
	call.Done <- call
}

/*
	Client包括：
	cc          encode.Codec // 编码器解码器
	opt         *server.Option
	sendingLock sync.Mutex // 发送锁，保证按顺序发送
	header      encode.Header
	mu          sync.Mutex       // client 自身锁
	seq         uint64           // 记录当前序列号
	pending     map[uint64]*Call // 未完成调用池
	closing     bool             // 被client叫停
	shutdown    bool             // 被server叫停
*/
type Client struct {
	cc          encode.Codec // 编码器解码器
	opt         *server.Option
	sendingLock sync.Mutex // 发送锁，保证按顺序发送
	header      encode.Header
	mu          sync.Mutex       // client 自身锁
	seq         uint64           // 记录当前序列号
	pending     map[uint64]*Call // 未完成调用池
	closing     bool             // 被client叫停
	shutdown    bool             // 被server叫停
}

// Client实现io.Closer接口检查
var _ io.Closer = (*Client)(nil)

var ErrorShutdown = errors.New("connection is shut down")

// 关闭编码器并设置closing为true
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing {
		return ErrorShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// 客户端状态，当closing和shutdown均为false时可用
func (client *Client) isAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.closing && !client.shutdown
}

// 注册调用，设置调用序列号，向调用池中添加当前调用
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	// 不能用!client.isAvailable()因为无法获取锁
	//if !client.isAvailable() {
	//	return 0, ErrorShutdown
	//}
	if client.closing || client.shutdown {
		return 0, ErrorShutdown
	}

	call.SequenceNum = client.seq
	client.pending[client.seq] = call
	client.seq++

	return call.SequenceNum, nil
}

// 从调用池中移除指定seq的调用
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 收到err信号，终止所有的调用
func (client *Client) terminateCalls(err error) {
	// 获取发送锁，占用发送
	client.sendingLock.Lock()
	defer client.sendingLock.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
	client.shutdown = true
}

/*接收，一般使用goroutine实现异步操作，有三种意外的可能：
	1. 由于通信错误等，返回一个空的call
	2. 服务端处理报错，错误会在Header中显示
	3. 解码器cc读取时报错
出错则立刻停止
*/
func (client *Client) receive() {
	var err error
	for err == nil {
		var h encode.Header
		if err := client.cc.ReadHeader(&h); err != nil {
			break
		} else {
			call := client.removeCall(h.Seq)
			switch {
			case call == nil:
				err = client.cc.ReadBody(nil)
			case h.Err != "":
				call.Error = errors.New(h.Err)
				call.done()
				err = call.Error
			default:
				err = client.cc.ReadBody(call.Reply)
				if err != nil {
					call.Error = errors.New("reading body" + err.Error())
				}
				call.done()
			}
		}
	}
	client.terminateCalls(err)
}

/*
新创建客户端包括：
	1. 根据编码类型生成cc
	2. 向server发送一个json包装等opt
	3. 初始化client，并创建一个client.receive携程
*/
func NewClient(conn net.Conn, opt *server.Option) (*Client, error) {
	// 配合超时测试
	// time.Sleep(time.Second * 2)
	f := encode.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		defer func() { _ = conn.Close() }()
		log.Println("rpc client: opt error:", err)
		return nil, err
	}

	client := &Client{
		seq:     1,
		cc:      f(conn),
		opt:     opt,
		pending: map[uint64]*Call{},
	}

	go client.receive()
	return client, nil
}

//先注册调用获得seq值，然后通过cc写来发送一个rpc的call给server
func (client *Client) send(call *Call) {

	client.sendingLock.Lock()
	defer client.sendingLock.Unlock()

	seq, err := client.registerCall(call)

	if err != nil {
		call.Error = err
		call.done()
		return
	}

	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = call.SequenceNum
	client.header.Err = ""

	err = client.cc.Write(&client.header, call.Args)
	time.Sleep(time.Millisecond * 500)
	if err != nil {
		call = client.removeCall(seq)
		call.Error = err
		call.done()
		return
	}

}

// 异步调用接口
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 20)
	} else if cap(done) == 0 {
		log.Panic("rpc client: channel unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}

	client.send(call)
	log.Println("client send over")
	return call
}

// 同步调用，限制channel长度为1实现阻塞
func (client *Client) CallWithTimeout(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	// select + ctx 实现多goroutine之间协同（withCancel）
	select {
	case <-ctx.Done():
		log.Println("call out of time")
		client.removeCall(call.SequenceNum)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		log.Println("call done")
		return call.Error
	}
}

// 创建上下文，开始计时
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	var ctx context.Context
	timeout := client.opt.HandleTimeOut
	ctx, _ = context.WithTimeout(context.Background(), timeout)
	return client.CallWithTimeout(ctx, serviceMethod, args, reply)
}

// 设置编码的格式，参数数量目前只支持1个
// TODO 实现多种编码格式后调整本函数
func defineOpt(options ...*server.Option) (opt *server.Option, err error) {
	if len(options) == 0 || options[0] == nil {
		return server.DefaultOption, nil
	}
	if len(options) > 1 {
		return nil, errors.New("too much opts")
	}
	opt = options[0]
	opt.MagicNumber = server.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = server.DefaultOption.CodecType
	}
	return opt, nil
}

type dialResult struct {
	client *Client
	err    error
}

// Dial 根据用户需求等option创建client对象
func Dial(network, address string, options ...*server.Option) (client *Client, err error) {
	opt, err := defineOpt(options...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeOut)
	if err != nil {
		return nil, err
	}

	// 退出时执行conn.Close()
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()

	// result Channel阻塞
	resultChan := make(chan dialResult)

	go func() {
		client, err = NewClient(conn, opt)
		resultChan <- dialResult{client: client, err: err}
	}()

	// 无限等待情况
	if opt.ConnectTimeOut == 0 {
		result := <-resultChan
		return result.client, result.err
	} else {
		// select + 时限chan + result chan实现限时
		select {
		case result := <-resultChan:
			return result.client, result.err
		case <-time.After(opt.ConnectTimeOut):
			return nil, errors.New("connection time out")
		}
	}
}

func NewHTTPClient(conn net.Conn, opt *server.Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", server.DefaultRPCPath))
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})

	if err == nil && resp.Status == server.ConnectedMessage {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// Dial的HTTP版本
func DialHTTP(network, address string, options ...*server.Option) (client *Client, err error) {
	opt, err := defineOpt(options...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeOut)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	resultChan := make(chan dialResult)

	go func() {
		client, err = NewHTTPClient(conn, opt)
		resultChan <- dialResult{client: client, err: err}
	}()

	// 与Dial相同的时间限制方法
	if opt.ConnectTimeOut == 0 {
		result := <-resultChan
		return result.client, result.err
	} else {
		select {
		case result := <-resultChan:
			return result.client, result.err
		case <-time.After(opt.ConnectTimeOut):
			return nil, errors.New("connection time out")
		}
	}
}

/*
MixDial
addr格式为:
	protocal@ip:port
例如：
	http@127.0.0.1:9999
	tcp@123.123.123.123:123
分流为TCPDial或是HTTPDial
TODO http2版本
*/
func MixDial(addr string, opts ...*server.Option) (client *Client, err error) {
	splitedAddr := strings.Split(addr, "@")
	if len(splitedAddr) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", addr)
	}

	protocal, address := splitedAddr[0], splitedAddr[1]
	switch protocal {
	case "http":
		return DialHTTP("tcp", address, opts...)
	default:
		return Dial(protocal, address, opts...)

	}

}
