package elasticlogrus

import (
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"time"
)

type Hook struct {
	client      *elastic.Client
	index       string
	docType     string
	levels      []logrus.Level
	ctx         context.Context
	cancel      context.CancelFunc
	formatter   Formatter
	flushFunc   flushFunc
	errorLogger ErrorLogger
}

type flushFunc func(*logrus.Entry, *Hook) error

type Formatter func(*logrus.Entry) Message
type Message map[string]interface{}
type ErrorLogger func(string, error)

func defaultFormatter(entry *logrus.Entry) Message {
	if e, ok := entry.Data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			entry.Data[logrus.ErrorKey] = err.Error()
		}
	}

	return Message{
		"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
		"message":    entry.Message,
		"level":      entry.Level.String(),
		"data":       entry.Data,
	}
}

func syncFlush(entry *logrus.Entry, hook *Hook) error {
	_, err := hook.client.
		Index().
		Index(hook.index).
		Type(hook.docType).
		BodyJson(hook.formatter(entry)).
		Do(hook.ctx)

	return err
}

func asyncFlush(entry *logrus.Entry, hook *Hook) error {
	go func() {
		err := syncFlush(entry, hook)
		if hook.errorLogger != nil {
			hook.errorLogger("couldn't send log to elastic", err)
		}
	}()

	return nil
}

func New(client *elastic.Client, index string) (*Hook, error) {
	ctx, cancel := context.WithCancel(context.Background())

	exists, err := client.IndexExists(index).Do(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	if !exists {
		cancel()
		return nil, errors.New("index not exists")
	}

	return &Hook{
		client:    client,
		index:     index,
		docType:   "doc",
		levels:    logrus.AllLevels,
		ctx:       ctx,
		cancel:    cancel,
		formatter: defaultFormatter,
		flushFunc: syncFlush,
	}, nil
}

func (h *Hook) SetLevel(level logrus.Level) {
	var levels []logrus.Level
	for _, l := range logrus.AllLevels {
		if l <= level {
			levels = append(levels, l)
		}
	}

	h.levels = levels
}

func (h *Hook) SetFormatter(formatter Formatter) {
	h.formatter = formatter
}

func (h *Hook) SetDocumentType(t string) {
	h.docType = t
}

func (h *Hook) EnableBulkFlush(options *BulkOptions) error {
	s := h.client.BulkProcessor().
		Name("elasticlogrus")

	if options.Workers > 0 {
		s.Workers(options.Workers)
	}

	if options.BulkActions != 0 {
		s.BulkActions(options.BulkActions)
	}

	if options.BulkSize != 0 {
		s.BulkSize(options.BulkSize)
	}

	if options.FlushInterval != 0 {
		s.FlushInterval(options.FlushInterval)
	}

	if options.Backoff != nil {
		s.Backoff(options.Backoff)
	}

	if options.RetryItemStatusCodes != nil {
		s.RetryItemStatusCodes(options.RetryItemStatusCodes...)
	}

	processor, err := s.Before(options.BulkBefore).
		After(options.BulkAfter).
		Do(context.Background())


	cancel := h.cancel
	h.cancel = func() {
		cancel()
		e := processor.Flush()
		if e != nil && h.errorLogger != nil {
			h.errorLogger("couldn't flush", e)
		}

		e = processor.Close()
		if e != nil && h.errorLogger != nil {
			h.errorLogger("couldn't close", e)
		}
	}

	h.flushFunc = func(entry *logrus.Entry, hook *Hook) error {
		req := elastic.NewBulkIndexRequest().
			Index(hook.index).
			Type(hook.docType).
			Doc(hook.formatter(entry))

		processor.Add(req)

		return nil
	}

	return err
}

func (h *Hook) EnableAsyncFlush() {
	h.flushFunc = asyncFlush
}

func (h *Hook) EnableSyncFlush() {
	h.flushFunc = syncFlush
}

func (h *Hook) Fire(entry *logrus.Entry) error {
	return h.flushFunc(entry, h)
}

func (h *Hook) Levels() []logrus.Level {
	return h.levels
}

func (h *Hook) Close() {
	h.cancel()
}