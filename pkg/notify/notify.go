package notify

import (
	"mtuned/pkg/config"
	"mtuned/pkg/log"
	"time"

	"go.uber.org/zap"
)

// Message structure of message sent to notify service
type Message struct {
	Subject string
	Content string
	Time    time.Time
}

// Sender sends notification
type Sender interface {
	send(string, string) error
}

type service struct {
	disabled   bool
	interval   int
	queue      chan Message
	msgRecords map[string]time.Time
	sender     Sender
}

// Service notify service
type Service struct {
	service
}

// NewService returns a new notify service
func NewService(cfg *config.Config) *Service {
	svc := service{
		disabled:   cfg.NotifyFrequency == config.NotifyFrequencyNever,
		queue:      make(chan Message, 10),
		msgRecords: make(map[string]time.Time),
	}
	if cfg.NotifyFrequency == config.NotifyFrequencyDaily {
		svc.interval = 24 * 60 * 60
	}

	if len(cfg.Email) > 0 {
		svc.sender = emailSender(cfg.Email, cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPHost, cfg.SMTPPort)
	} else if len(cfg.SlackWebhook) > 0 {
		svc.sender = slackSender(cfg.SlackWebhook)
	} else if len(cfg.GenericWebhook) > 0 {
		svc.sender = genericSender(cfg.GenericWebhook)
	}

	return &Service{
		service: svc,
	}
}

// Run runs notify service
func (s *Service) Run() {
	for msg := range s.queue {
		if s.disabled || s.sender == nil {
			continue
		}

		record, ok := s.msgRecords[msg.Subject]
		if ok && record.Add(time.Duration(s.interval)*time.Second).After(msg.Time) {
			continue
		}

		log.Logger().Info("[Notify]",
			zap.String("subject", msg.Subject),
			zap.String("content", msg.Content),
			zap.String("time", msg.Time.String()))

		err := s.sender.send(msg.Subject, msg.Content)
		if err != nil {
			log.Logger().Error("failed to send notify",
				zap.NamedError("error", err), zap.String("subject", msg.Subject), zap.String("content", msg.Content))
		} else {
			s.msgRecords[msg.Subject] = msg.Time
		}
	}
}

// Notify notify notify service of advise
func (s *Service) Notify(msg Message) {
	s.queue <- msg
}
