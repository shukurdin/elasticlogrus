package elasticlogrus

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"
)

type NewHookFunc func(client *elastic.Client, index string) (*Hook, error)

const testElasticHost = "http://localhost:9200"

func TestHook_WithSyncFlush(t *testing.T) {
	if err := simpleTest("sync", New); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestHook_WithAsyncFlush(t *testing.T) {
	if err := simpleTest("async", newHookWithAsyncFlush); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestHook_WithBulkFlush(t *testing.T) {
	if err := simpleTest("bulk", newHookWithBulkFlush); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestHook_WriteError(t *testing.T) {
	logger, client, err := createLoggerWithClient(testElasticHost, New, "error")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	logger.WithError(errors.New("this is error")).
		Error("error sample")

	time.Sleep(2 * time.Second)
	searchResult, err := client.Search("error").Do(context.TODO())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if searchResult.TotalHits() != int64(1) {
		t.Error("no log created")
		t.FailNow()
	}

	data := searchResult.Each(reflect.TypeOf(logrus.Entry{}))
	for _, d := range data {
		if e, ok := d.(logrus.Entry); ok {
			if errData, exists := e.Data[logrus.ErrorKey]; !exists && errData != "this is error" {
				t.Error("no error found")
				t.FailNow()
			}
		}
	}
}

func TestHook_WithCustomFormatter(t *testing.T) {
	index := "custom-formatter"
	newHookFunc := func(client *elastic.Client, index string) (*Hook, error) {
		hook, err := New(client, index)
		if err != nil {
			return nil, err
		}

		hook.SetFormatter(func(entry *logrus.Entry) Message {
			return Message{
				"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
				"message":    entry.Message,
				"level":      entry.Level.String(),
				"data":       entry.Data,
				"component":  "test",
			}
		})

		return hook, nil
	}

	logger, client, err := createLoggerWithClient(testElasticHost, newHookFunc, index)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	samples := 100
	for i := 0; i < samples; i++ {
		logger.Info("Hello world!")
	}

	time.Sleep(2 * time.Second)

	query := elastic.NewTermQuery("component", "test")
	searchResult, err := client.Search(index).
		Query(query).
		Do(context.TODO())

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if searchResult.TotalHits() != int64(samples) {
		t.Errorf("not all logs pushed to elastic: expected %d got %d", samples, searchResult.TotalHits())
		t.FailNow()
	}
}

func simpleTest(index string, newHookFunc NewHookFunc) error {
	logger, client, err := createLoggerWithClient(testElasticHost, newHookFunc, index)
	if err != nil {
		return err
	}

	samples := 100
	for i := 0; i < samples; i++ {
		logger.Info("Hello world!")
	}

	time.Sleep(2 * time.Second)

	query := elastic.NewMatchQuery("message", "Hello world!")
	searchResult, err := client.Search(index).
		Query(query).
		Do(context.TODO())

	if err != nil {
		return errors.Wrap(err, "search")
	}

	if searchResult.TotalHits() != int64(samples) {
		return errors.Errorf("not all logs pushed to elastic: expected %d got %d", samples, searchResult.TotalHits())
	}

	return nil
}

func createLoggerWithClient(url string, newHookFunc NewHookFunc, index string) (*logrus.Logger, *elastic.Client, error) {
	if r, err := http.Get(url); err != nil {
		return nil, nil, errors.Wrap(err, "elastic not reachable")
	} else {
		buf, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()
		fmt.Println(string(buf))
	}

	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false))

	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create elastic client")
	}

	exists, err := client.IndexExists(index).Do(context.TODO())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't check index")
	}

	if exists {
		_, err = client.DeleteIndex(index).Do(context.TODO())
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't delete index")
		}
	}

	_, err = client.CreateIndex(index).Do(context.TODO())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't index")
	}

	hook, err := newHookFunc(client, index)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create hook")
	}

	logger := logrus.New()
	logger.AddHook(hook)

	return logger, client, nil
}

func newHookWithAsyncFlush(client *elastic.Client, index string) (*Hook, error) {
	hook, err := New(client, index)
	if err != nil {
		return nil, err
	}

	hook.EnableAsyncFlush()

	return hook, err
}

func newHookWithBulkFlush(client *elastic.Client, index string) (*Hook, error) {
	hook, err := New(client, index)
	if err != nil {
		return nil, err
	}

	err = hook.EnableBulkFlush(&BulkOptions{
		Workers:       2,
		FlushInterval: time.Second,
	})

	if err != nil {
		return nil, err
	}

	return hook, err
}
