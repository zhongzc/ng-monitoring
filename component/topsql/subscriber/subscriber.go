package subscriber

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"
)

type Subscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func NewSubscriber(
	cfg *config.Config,
	topoSubscriber topology.Subscriber,
	varSubscriber pdvariable.Subscriber,
	cfgSubscriber config.Subscriber,
	store store.Store,
) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go utils.GoWithRecovery(func() {
		defer wg.Done()
		sm := NewManager(ctx, wg, cfg, varSubscriber, topoSubscriber, cfgSubscriber, store)
		sm.Run()
	}, nil)

	return &Subscriber{
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
	}
}

func (ds *Subscriber) Close() {
	log.Info("stopping scrapers")
	ds.cancel()
	ds.wg.Wait()
	log.Info("stop scrapers successfully")
}
