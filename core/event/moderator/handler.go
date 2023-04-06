package moderator

import "github.com/goto/salt/log"

type Event interface {
	Bytes() ([]byte, error)
}

type EventHandler struct {
	messageChan chan<- []byte
	logger      log.Logger
}

func NewEventHandler(messageChan chan<- []byte, logger log.Logger) *EventHandler {
	return &EventHandler{
		messageChan: messageChan,
		logger:      logger,
	}
}

func (e EventHandler) HandleEvent(event Event) {
	bytes, err := event.Bytes()
	if err != nil {
		e.logger.Error("error converting event to bytes: %v", err)
		return
	}
	go func() { e.messageChan <- bytes }()
}
