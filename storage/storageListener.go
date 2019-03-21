package storage

import (
	"sync"
)

// StorageListener listens on a storage Request channel and assigns an appropriate RequstHandler.
type StorageListener interface {
	GetCommunicationChannel() chan *Request
}

// Listener implements StorageListener.
type Listener struct {
	RequestHandlers map[RequestConstant]RequestHandler
	requestChannel  chan *Request
	quitChannel     chan struct{}
	wg              sync.WaitGroup
}

// GetCommunicationChannel returns the storage Request channel.
func (l *Listener) GetCommunicationChannel() chan *Request {
	return l.requestChannel
}

// Listen starts the listen process and begins servicing storage Requests until Stop is called.
func (l *Listener) Listen() {
	l.wg.Add(1)
	go func() {
		for {
			select {
			case request := <-l.requestChannel:
				handler, ok := l.RequestHandlers[request.RequestType]
				switch {
				case !ok:
					NoopHandler(request)
				case handler == nil:
					NoopHandler(request)
				default:
					handler(request)
				}
			case <-l.quitChannel:
				l.wg.Done()
				return
			}
		}
	}()
}

// Stop quits the listen process.
func (l *Listener) Stop() {
	close(l.quitChannel)
	l.wg.Wait()
}

// NewListener returns a new Listener with an empty HandleRequestMap and Request channel.
func NewListener() *Listener {
	return &Listener{
		RequestHandlers: HandleRequestMap,
		requestChannel:  make(chan *Request),
		quitChannel:     make(chan struct{}),
		wg:              sync.WaitGroup{},
	}
}

// NewListenerChannel returns a new Listener with the given Request channel and an empty HandleRequestMap.
func NewListenerChannel(requestChannel chan *Request) *Listener {
	return &Listener{
		RequestHandlers: HandleRequestMap,
		requestChannel:  requestChannel,
		quitChannel:     make(chan struct{}),
		wg:              sync.WaitGroup{},
	}
}
