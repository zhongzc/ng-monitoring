package scraper

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Client interface {
	Connect(ctx context.Context, tlsConfig *tls.Config) (Channel, error)
}

type Channel interface {
	BlockingReceive() (interface{}, error)
	Close()
}

type Handler interface {
	Handle(interface{}) error
}

type Scraper struct {
	job       string
	component topology.Component

	ctx    context.Context
	cancel context.CancelFunc

	tlsConfig *tls.Config

	client  Client
	handler Handler
}

func NewScraper(
	job string,
	component topology.Component,
	ctx context.Context,
	tlsConfig *tls.Config,
	client Client,
	handler Handler,
) *Scraper {
	ctx, cancel := context.WithCancel(ctx)
	return &Scraper{
		job:       job,
		component: component,
		ctx:       ctx,
		cancel:    cancel,
		tlsConfig: tlsConfig,
		client:    client,
		handler:   handler,
	}
}

var _ subscriber.Scraper = &Scraper{}

func (s *Scraper) Run() {
	log.Info("starting to scrape from the component",
		zap.String("job", s.job),
		zap.Any("component", s.component))
	defer func() {
		s.cancel()
		log.Info("stop scraping from the component",
			zap.String("job", s.job),
			zap.Any("component", s.component))
	}()

	bo := newBackoffScrape(s.ctx, s.tlsConfig, s.job, s.component, s.client)
	defer bo.close()

	for {
		record, ok := bo.scrape()
		if !ok {
			return
		}
		if err := s.handler.Handle(record); err != nil {
			log.Warn("failed to handle the record",
				zap.String("job", s.job),
				zap.Any("component", s.component),
				zap.Error(err))
		}
	}
}

func (s *Scraper) IsDown() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *Scraper) Close() {
	s.cancel()
}

type backoffScrape struct {
	ctx    context.Context
	tlsCfg *tls.Config

	job       string
	component topology.Component

	client  Client
	channel Channel

	firstWaitTime time.Duration
	maxRetryTimes uint
}

func newBackoffScrape(
	ctx context.Context,
	tlsCfg *tls.Config,
	job string,
	component topology.Component,
	client Client,
) *backoffScrape {
	return &backoffScrape{
		ctx:    ctx,
		tlsCfg: tlsCfg,

		job:       job,
		component: component,

		client: client,

		firstWaitTime: 2 * time.Second,
		maxRetryTimes: 8,
	}
}

func (bo *backoffScrape) scrape() (record interface{}, ok bool) {
	if bo.channel != nil {
		record, err := bo.channel.BlockingReceive()
		if err == nil {
			return record, true
		}
	}

	return bo.backoffScrape()
}

func (bo *backoffScrape) backoffScrape() (record interface{}, ok bool) {
	utils.WithRetryBackoff(bo.ctx, bo.maxRetryTimes, bo.firstWaitTime,
		func(retried uint) bool {
			if retried != 0 {
				log.Warn("retry to scrape component",
					zap.String("job", bo.job),
					zap.Any("component", bo.component),
					zap.Uint("retried", retried))
			}

			if bo.channel != nil {
				bo.channel.Close()
			}

			var err error
			bo.channel, err = bo.client.Connect(bo.ctx, bo.tlsCfg)
			if err != nil {
				log.Warn("failed to connect scrape target",
					zap.String("job", bo.job),
					zap.Any("component", bo.component),
					zap.Error(err))
				return false
			}

			record, err = bo.channel.BlockingReceive()
			if err != nil {
				log.Warn("failed to receive record",
					zap.String("job", bo.job),
					zap.Any("component", bo.component),
					zap.Error(err))
				return false
			}

			ok = true
			return true
		})

	return
}

func (bo *backoffScrape) close() {
	if bo.channel != nil {
		bo.channel.Close()
		bo.channel = nil
	}
}
