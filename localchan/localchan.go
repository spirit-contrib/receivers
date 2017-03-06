package localchan

import (
	"github.com/gogap/errors"
	"sync"
	"time"

	"github.com/gogap/spirit"
)

var (
	messageChans = make(map[string]<-chan spirit.ComponentMessage)
)

func init() {
	spirit.RegisterMessageReceivers(new(LocalChan))
}

func RegisterMessageChan(name string, msgChan <-chan spirit.ComponentMessage) {
	if len(name) == 0 {
		panic("localchan message chan could not be empty")
	}
	messageChans[name] = msgChan
}

type LocalChan struct {
	url string

	recvLocker sync.Mutex

	isRunning bool

	status spirit.ComponentStatus

	inPortName    string
	componentName string

	onMsgReceived   spirit.OnReceiverMessageReceived
	onReceiverError spirit.OnReceiverError

	messageChan <-chan spirit.ComponentMessage

	messageOnProcess sync.WaitGroup
}

func NewLocalChan(url string) spirit.MessageReceiver {
	return &LocalChan{
		url: url,
	}
}

func (p *LocalChan) Init(url string, options spirit.Options) (err error) {
	p.url = url

	var exist bool
	if p.messageChan, exist = messageChans[url]; !exist {
		err = spirit.ERR_OPTIONS_KEY_NOT_EXIST.New(errors.Params{"key": url})
		return
	}

	return
}

func (p *LocalChan) Type() string {
	return "localchan"
}

func (p *LocalChan) Metadata() spirit.ReceiverMetadata {
	return spirit.ReceiverMetadata{
		ComponentName: p.componentName,
		PortName:      p.inPortName,
		Type:          p.Type(),
	}
}

func (p *LocalChan) Address() spirit.MessageAddress {
	return spirit.MessageAddress{Type: p.Type(), Url: p.url}
}

func (p *LocalChan) BindInPort(componentName, inPortName string, onMsgReceived spirit.OnReceiverMessageReceived, onReceiverError spirit.OnReceiverError) {
	p.inPortName = inPortName
	p.componentName = componentName
	p.onMsgReceived = onMsgReceived
	p.onReceiverError = onReceiverError
}

func (p *LocalChan) IsRunning() bool {
	return p.isRunning
}

func (p *LocalChan) Stop() {
	p.recvLocker.Lock()
	defer p.recvLocker.Unlock()

	if !p.isRunning {
		return
	}

	p.isRunning = false
}

func (p *LocalChan) Start() {
	p.recvLocker.Lock()
	defer p.recvLocker.Unlock()

	if p.isRunning {
		return
	}

	go func() {
		lastStatUpdated := time.Now()
		statUpdateFunc := func() {
			if time.Now().Sub(lastStatUpdated).Seconds() >= 1 {
				lastStatUpdated = time.Now()
				spirit.EventCenter.PushEvent(spirit.EVENT_RECEIVER_MSG_COUNT_UPDATED, p.Metadata(), []spirit.ChanStatistics{
					{"receiver_message", len(p.messageChan), cap(p.messageChan)},
					{"receiver_error", 0, 0},
				})
			}
		}

		for {
			select {
			case compMsg, ok := <-p.messageChan:
				{
					if !ok {
						return
					}

					p.onMsgReceived(p.inPortName, nil, compMsg)
					spirit.EventCenter.PushEvent(spirit.EVENT_RECEIVER_MSG_RECEIVED, p.Metadata(), compMsg)
					statUpdateFunc()
				}
			case <-time.After(time.Second):
				{
					if len(p.messageChan) == 0 && !p.isRunning {
						return
					}
				}
			}
		}
	}()
}
