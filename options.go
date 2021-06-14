package elasticlogrus

import (
	"github.com/olivere/elastic/v7"
	"time"
)

type BulkOptions struct {
	Workers int
	Stats   bool

	BulkActions   int
	BulkSize      int
	FlushInterval time.Duration

	Backoff    elastic.Backoff
	BulkBefore elastic.BulkBeforeFunc
	BulkAfter  elastic.BulkAfterFunc

	RetryItemStatusCodes []int
}
