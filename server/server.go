package server

import (
	"encode"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MagicNumber = 0x3bef5c

const (
	ConnectedMessage = "200 Connected"
	DefaultRPCPath   = "/"
)

// 处理一下连接超时，默认10s，若ConnectTimeOut=0，则为无限等待
type Option struct {
	MagicNumber    int
	CodecType      string
	ConnectTimeOut time.Duration
	HandleTimeOut  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      encode.JsonType,
	ConnectTimeOut: time.Second * 10,
	HandleTimeOut:  time.Second * 60,
}

type methodType struct {
	Method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

// 注意此处的细节区别
func (m *methodType) NewArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) NewReplyv() reflect.Value {
	// reply must be a pointer type

	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

/*
Service 每一个类（type）对应一个Service。
	name为类名，<rtype, rvalue>为类的反射。
	Method为类中包含方法。
*/
type Service struct {
	name   string
	rtype  reflect.Type
	rvalue reflect.Value
	Method map[string]*methodType
}

func NewService(obj interface{}) *Service {
	s := new(Service)
	s.rtype = reflect.TypeOf(obj)

	s.rvalue = reflect.ValueOf(obj)
	s.name = reflect.Indirect(s.rvalue).Type().Name()

	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}

	s.registerMethods()

	return s
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

/*
满足条件：
	1. 首字母大写
	2. 只有三个输入一个输出
*/
func (service *Service) registerMethods() {
	service.Method = make(map[string]*methodType)
	for i := 0; i < service.rtype.NumMethod(); i++ {
		method := service.rtype.Method(i)

		mType := method.Type
		// 三输入(self, args, reply)一输出，其中第一个是自身
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}

		// 输出一定是error类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		service.Method[method.Name] = &methodType{
			Method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", service.name, method.Name)
	}
}

/*
Call 调用f(type.value, inputs, outputs)
*/
func (service *Service) Call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.Method.Func
	returnValues := f.Call([]reflect.Value{service.rvalue, argv, replyv})

	if err := returnValues[0].Interface(); err != nil {
		return err.(error)
	}
	return nil
}

// Server 需要一个服务映射，并具有并发读写安全，实现服务注册。
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

func (server *Server) Register(obj interface{}) error {
	service := NewService(obj)
	if _, dup := server.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("rpc: service already defined: " + service.name)
	}
	return nil
}

/*
FindService Service.Method，通过查找serviceMap找寻service，查找service.Method寻找method。
*/
func (server *Server) FindService(serviceMethod string) (service *Service, mType *methodType, err error) {
	// 找到最后一个.
	dotPos := strings.LastIndex(serviceMethod, ".")
	if dotPos < 0 {
		err = errors.New("rpc server: invalid service method: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dotPos], serviceMethod[dotPos+1:]
	serviceInterface, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service: " + serviceMethod)
		return
	}
	service = serviceInterface.(*Service)
	mType = service.Method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method: " + serviceMethod)
		return
	}
	return
}

func (server *Server) Accept(listener net.Listener) {
	for {
		// 开始监听
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept error")
			return
		}
		// 收到一个连接后创建协程进行处理
		go server.ServeConn(conn)
	}
}

func (server *Server) ServeConn(conn io.ReadWriteCloser) {

	defer func() { _ = conn.Close() }()
	var opt Option
	// 判断格式是否正确
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 判断魔数是否正确
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 根据编码类型选择编码函数f
	f := encode.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// 根据编码函数提供服务
	server.serveCodec(f(conn))
}

var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc encode.Codec) {

	sending := new(sync.Mutex) // 上锁
	wg := new(sync.WaitGroup)  // 创建wg
	for {
		// 判断request是否可读

		req, err := server.readRequest(cc)
		if err != nil {
			// 读完请求
			if req == nil {
				break
			}
			req.h.Err = err.Error()
			// 不可读直接返回错误

			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		// wg +1并创建一个goroutine进行handle
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, time.Second)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *encode.Header // header of request
	argv, replyv reflect.Value  // argv and replyv of request
	mType        *methodType
	service      *Service
}

func (server *Server) readRequestHeader(cc encode.Codec) (*encode.Header, error) {
	var h encode.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &h, nil
}

/*
使用编码器cc反序列化报文得到argv
*/
func (server *Server) readRequest(cc encode.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.service, req.mType, err = server.FindService(h.ServiceMethod)
	if err != nil {
		return nil, err
	}
	req.argv = req.mType.NewArgv()
	req.replyv = req.mType.NewReplyv()

	argvPtr := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvPtr = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvPtr); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

func (server *Server) sendResponse(cc encode.Codec, h *encode.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc encode.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	// 和handle前的wg+1对应
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.service.Call(req.mType, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Err = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	// 没有对sendResponse做时限处理，call后会一直等待sendResponse成功
	select {
	case <-called:
		<-sent
	case <-time.After(timeout):
		req.h.Err = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	}
}

func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 Method not connect\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Println("rpc hijacking "+req.RemoteAddr+": ", err.Error())
		return
	}
	// 添加HTTP报文头
	_, _ = io.WriteString(conn, "HTTP/1.0 "+ConnectedMessage+"\n\n")
	server.ServeConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, server)
}
