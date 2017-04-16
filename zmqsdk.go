package zmqsdk

import (
	"errors"
	"fmt"
	l4g "github.com/alecthomas/log4go"
	pkgzmq "github.com/pebbe/zmq4"
	dzhyun "gw.com.cn/dzhyun/dzhyun.git"
	
	"sync"
	"syscall"
	"time"
)

/*****************************变量********************************/
const (
	Default_Chan_size       = 1024
	Default_Reactor_timeout = time.Millisecond * 100
	Default_CMD             = 0
	Default_MaxReconnectIvl    = 90
)

type SNetData struct {
	Data []byte
}

type RNetData struct {
	Msg  *dzhyun.ExchInnerResMsg
}

type INetDataProc interface {
	Proc(netData *RNetData) error
}

type IEventProc interface {
	Proc(ev pkgzmq.Event, sdk* ZmqSdk)
}

type GenRNetDataProc func(data []byte) (*RNetData, error)


/*********************************DefaultZmqMgr***************************/
var DefaultZmqSdk ZmqSdk

func Init(name string) error {
	err := DefaultZmqSdk.Init(name)
	return err
}

func Start(cmAddr string) error {
	err := DefaultZmqSdk.Start(cmAddr)
	return err
}

func Stop() error {
	err := DefaultZmqSdk.Stop()
	return err
}

func SendMsg(data []byte) error {
	err := DefaultZmqSdk.SendMsg(data)
	return err
}

func Reg(cmd int32, netDataProc INetDataProc) {
	DefaultZmqSdk.Reg(cmd, netDataProc)
}


func UseRevChan() chan *RNetData {
	ch := DefaultZmqSdk.UseRevChan()
	return ch
}


/******************************ZmqSDK异步处理*******************************/
type ZmqSdk struct {
	mRSoc      *pkgzmq.Socket //Router socket
	mReactor   *pkgzmq.Reactor
	mRcvChan   chan *RNetData                  //暴露给上层的数据接收chan
	mSndChan   chan interface{}                //内部发送chan
	mDataProc  map[int32]INetDataProc //网络Msg的处理函数
	mEventProc IEventProc
	mGenRNetDataProc   GenRNetDataProc         //网络数据转换成RNetData
	mRegFlag   bool                            //上层是否使用mRcvChan
	mRunFlag   bool                            //运行标志
	mState     bool                            //健康状态
	mStateLock sync.RWMutex
	mName      string                          //名字不能重复
	mThrowFlag bool                            //chan满时丢弃标志
	mWaitGroup sync.WaitGroup
}

func (this * ZmqSdk) GetName() string {
	return this.mName
}

func (this * ZmqSdk) GetState()bool{
	this.mStateLock.RLock()
	defer this.mStateLock.RUnlock()
	return this.mState
}

func (this *ZmqSdk) Init(name string) (err error) {
	this.mName = name
	this.mRcvChan = make(chan *RNetData, Default_Chan_size)
	this.mSndChan = make(chan interface{}, Default_Chan_size)
	this.mDataProc = make(map[int32]INetDataProc, 0)
	if this.mRSoc, err = pkgzmq.NewSocket(pkgzmq.DEALER); err != nil {
		return err
	}
	return nil
}

func (this *ZmqSdk) Start(cmAddr string) (err error) {
	l4g.Info("ZmqSdk ConnectAddr[%s]", cmAddr)
	if err := this.setSocketProperty(this.mRSoc); err != nil {
		return err
	}
	if err := this.mRSoc.Connect(cmAddr); err != nil {
		return err
	}
	if err := this.run(); err != nil {
		return err
	}
	this.mRunFlag = true
	l4g.Info("ZmqSdk Start Ok")
	return nil
}

func (this *ZmqSdk) Stop() error {
	l4g.Info("ZmqSdk Stop")
	this.mRegFlag = false
	if this.mSndChan != nil {
		this.mSndChan <- nil
	}
	if this.mRSoc != nil {
		this.mRSoc.Close()
	}
	if this.mRunFlag {
		this.mWaitGroup.Wait()
	}
	if this.mRcvChan != nil {
		close(this.mRcvChan)
		this.mRcvChan = nil
	}
	if this.mSndChan != nil {
		close(this.mSndChan)
		this.mSndChan = nil
	}
	this.mRunFlag = false
	l4g.Info("ZmqSdk Stop Ok")
	return nil
}

//handler 可以传进来
func (this *ZmqSdk) SendMsg(data []byte) error {
	msg := &SNetData{
		Data:data,
	}
	
	if !this.mThrowFlag {
		this.mSndChan <- msg
		return nil
	}
	
	select {
	case this.mSndChan <- msg:
	default:
		return fmt.Errorf("SndChan full so throw Msg")
	}
	return nil
}

func (this *ZmqSdk) Reg(cmd int32, netDataProc INetDataProc) {
	this.mDataProc[cmd] = netDataProc
}

func (this *ZmqSdk) RegRNetDataGen(p GenRNetDataProc){
	this.mGenRNetDataProc = p
}

func (this *ZmqSdk) RegEventProc(p IEventProc) {
	this.mEventProc = p
}

func (this *ZmqSdk) UseRevChan() chan *RNetData {
	this.mRegFlag = true
	return this.mRcvChan
}


/********************************内部接口********************************/
func (this *ZmqSdk) run() error {
	this.mReactor = pkgzmq.NewReactor()
	if this.mReactor == nil {
		return errors.New("zmq NewReactor nil")
	}
	this.mReactor.AddSocket(this.mRSoc, pkgzmq.POLLIN, this.dealRSocket)
	this.mReactor.AddChannel(this.mSndChan, 0, this.dealSndChan)

	go func() {
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		for {
			if err := this.mReactor.Run(Default_Reactor_timeout); err != nil {
				l4g.Info("Stop Reactor Because Run Err[%s]", err)
				switch pkgzmq.AsErrno(err) {
				case pkgzmq.Errno(syscall.EINTR): //被信号打断
					l4g.Info("ZmqMgr run err:%s", err.Error())
					continue
				case pkgzmq.Errno(syscall.EAGAIN): //资源不可用
					l4g.Info("ZmqMgr run err:%s", err.Error())
					continue
				}
				break
			}
		}
	}()
	return nil
}

func (this *ZmqSdk) dealSndChan(interf interface{}) error {
	switch interf.(type) {
	case nil:
		return errors.New("SndChan Recv nil")
	}
	netData, ok := interf.(*SNetData)
	if !ok {
		return errors.New("SndChan Recv Not SNetData")
	}
	if _, err := SendSocketNoEInter(this.mRSoc, netData.Data, pkgzmq.DONTWAIT); err != nil {
		l4g.Error("Send Router Soc err[%s]", err)
		return nil
	}
	return nil
}

func (this *ZmqSdk) dealRSocket(events pkgzmq.State) error {
	data, derr := RecvSocketNoEInter(this.mRSoc, pkgzmq.DONTWAIT)
	if derr != nil {
		return derr
	}
	if len(data) == 0 {
		return nil
	}
	
	newMsg, err := this.mGenRNetDataProc(data)
	if  err != nil{
		l4g.Error("RNetDataGen err[%s]", err)
		return nil
	}
	
	if this.mRegFlag {
		if !this.mThrowFlag {
			this.mRcvChan <- newMsg
			return nil
		}
		select {
		case this.mRcvChan <- newMsg:
		default:
			l4g.Warn("RcvChan full so throw Msg")
		}
		return nil
	}
	iProc, pok := this.mDataProc[newMsg.Msg.Cmd]
	if !pok || iProc == nil {
		iProc, pok = this.mDataProc[Default_CMD]
		if !pok || iProc == nil {
			l4g.Error("Unknown Msg err")
			return nil
		}
	}
	if err := iProc.Proc(newMsg); err != nil {
		l4g.Error("Proc Msg(%v) err[%s]", newMsg.Msg.Cmd, err)
	}
	return nil
}

//设置zmqsocket属性 后续加入验证功能
func (this *ZmqSdk) setSocketProperty(socket *pkgzmq.Socket) error {
	if err := socket.SetLinger(0); err != nil {
		return err
	}
	if err := socket.SetRcvhwm(0); err != nil {
		return err
	}
	if err := socket.SetSndhwm(0); err != nil {
		return err
	}
	if err := socket.SetRcvtimeo(0); err != nil {
		return err
	}
	if err := socket.SetSndtimeo(0); err != nil {
		return err
	}
	if err := socket.SetReconnectIvlMax(time.Duration(time.Second * Default_MaxReconnectIvl));err != nil {
		return err
	}
	if err := this.socMonitor(socket, this.mName); err != nil {
		return err
	}
	return nil
}

func (this *ZmqSdk) socMonitor(monSocket *pkgzmq.Socket, socName string) error {
	addr := fmt.Sprintf("inproc://%s.monitor%v.inproc", socName, time.Now().UnixNano() )
	if err := monSocket.Monitor(addr, pkgzmq.EVENT_CONNECTED|pkgzmq.EVENT_CLOSED|pkgzmq.EVENT_DISCONNECTED|pkgzmq.EVENT_MONITOR_STOPPED); err != nil {
		return err
	}
	s, err := pkgzmq.NewSocket(pkgzmq.PAIR)
	if err != nil {
		return err
	}
	err = s.Connect(addr)
	if err != nil {
		return err
	}
	go func() {
		defer s.Close()
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		for {
			a, b, c, err := s.RecvEvent(0)
			if err != nil {
				errno1 := pkgzmq.AsErrno(err)
				switch errno1 {
				case pkgzmq.Errno(syscall.EAGAIN):
					continue
				case pkgzmq.Errno(syscall.EINTR):
					continue
				default:
					l4g.Debug("zmq RecvEvent Get err %v, %d!", errno1, errno1)
				}
			}

			if c == 0 {
				continue
			}

			switch a {
			case pkgzmq.EVENT_LISTENING:
				l4g.Info("%s monitor event[%d][%s][%d] LISTENING", socName, a, b, c)
			case pkgzmq.EVENT_BIND_FAILED:
				l4g.Info("%s monitor event[%d][%s][%d] BIND_FAILED", socName, a, b, c)
				return
			case pkgzmq.EVENT_ACCEPTED:
				l4g.Info("%s monitor event[%d][%s][%d] ACCEPTED", socName, a, b, c)
			case pkgzmq.EVENT_ACCEPT_FAILED:
				l4g.Info("%s monitor event[%d][%s][%d] ACCEPT_FAILED", socName, a, b, c)
			case pkgzmq.EVENT_CONNECTED:
				l4g.Info("%s monitor event[%d][%s][%d] CONNECTED", socName, a, b, c)
				this.mStateLock.Lock()
				this.mState = true
				this.mStateLock.Unlock()
			case pkgzmq.EVENT_DISCONNECTED:
				l4g.Info("%s monitor event[%d][%s][%d] DISCONNECTED", socName, a, b, c)
				this.mStateLock.Lock()
				this.mState = false
				this.mStateLock.Unlock()
			case pkgzmq.EVENT_CLOSED://第一次连不上是closed，连上了断开是Disconnected
				l4g.Info("%s monitor event[%d][%s][%d] CLOSED", socName, a, b, c)
				this.mStateLock.Lock()
				this.mState = false
				this.mStateLock.Unlock()
			case pkgzmq.EVENT_MONITOR_STOPPED:
				l4g.Info("%s monitor event[%d][%s][%d] MONITOR_STOPPED", socName, a, b, c)
				this.mStateLock.Lock()
				this.mState = false
				this.mStateLock.Unlock()
				return
			default:
				l4g.Debug("%s monitor unknow event[%d][%s][%d]", socName, a, b, c)
			}
			if this.mEventProc != nil {
				this.mEventProc.Proc(a, this)
			}
		}

	}()
	return nil
}

func SendSocketNoEInter(soc *pkgzmq.Socket, datas []byte, flags pkgzmq.Flag) (int, error) {
GOTOEINTER:
	size, err := soc.SendBytes(datas, flags)
	if err != nil {
		switch pkgzmq.AsErrno(err) {
		case pkgzmq.Errno(syscall.EINTR):
			l4g.Debug("SendSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.ENOTSOCK:
			l4g.Debug("SendSocketNoEInter err:%s", err)
			return 0, nil
			//		case pkgzmq.Errno(syscall.EAGAIN):
			//			l4g.Debug("SendSocketNoEInter err:%s", err)
			//			goto GOTOEINTER
		default:
			l4g.Info("SendSocketNoEInter send err:%s", err)
		}
	}
	return size, err
}

func RecvSocketNoEInter(soc *pkgzmq.Socket, flags pkgzmq.Flag) ([]byte, error) {
GOTOEINTER:
	datas, err := soc.RecvBytes(flags)
	if err != nil {
		switch pkgzmq.AsErrno(err) {
		case pkgzmq.Errno(syscall.EINTR): //被信号打断
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.Errno(syscall.EAGAIN): //资源不可用
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.ENOTSOCK: //socket关闭了
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			return nil, nil
		default:
			l4g.Info("RecvSocketNoEInter recv err:%s", err)
		}
	}
	return datas, err
}
