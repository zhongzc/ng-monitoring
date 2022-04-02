package subscriber

import (
	"context"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"go.uber.org/zap"
	"time"
)

var (
	instancesItemSliceP = &instancesItemSlicePool{}
)

func NewSubscriber(
	topoSubscriber topology.Subscriber,
	varSubscriber pdvariable.Subscriber,
	cfgSubscriber config.Subscriber,
	store store.Store,
) *subscriber.Subscriber {
	controller := NewSubscriberController(store)
	return subscriber.NewSubscriber(
		topoSubscriber,
		varSubscriber,
		cfgSubscriber,
		controller,
	)
}

type SubscriberController struct {
	store store.Store

	config     *config.Config
	variable   *pdvariable.PDVariable
	components []topology.Component
}

func NewSubscriberController(store store.Store) *SubscriberController {
	cfg := config.GetDefaultConfig()
	variable := pdvariable.DefaultPDVariable()
	return &SubscriberController{
		store:    store,
		config:   &cfg,
		variable: variable,
	}
}

var _ subscriber.SubscribeController = &SubscriberController{}

func (tmc *SubscriberController) NewScraper(ctx context.Context, component topology.Component) subscriber.Scraper {
	return NewScraper(ctx, component, tmc.store, tmc.config.Security.GetTLSConfig())
}

func (tmc *SubscriberController) Name() string {
	return "Top SQL"
}

func (tmc *SubscriberController) IsEnabled() bool {
	return tmc.variable.EnableTopSQL
}

func (tmc *SubscriberController) UpdatePDVariable(variable pdvariable.PDVariable) {
	tmc.variable = &variable
}

func (tmc *SubscriberController) UpdateConfig(cfg config.Config) {
	tmc.config = &cfg
}

func (tmc *SubscriberController) UpdateTopology(components []topology.Component) {
	tmc.components = components

	if tmc.variable.EnableTopSQL {
		if err := tmc.storeTopology(); err != nil {
			log.Warn("failed to store topology", zap.Error(err))
		}
	}
}

func (tmc *SubscriberController) storeTopology() error {
	if len(tmc.components) == 0 {
		return nil
	}

	items := instancesItemSliceP.Get()
	defer instancesItemSliceP.Put(items)

	now := time.Now().Unix()
	for _, com := range tmc.components {
		switch com.Name {
		case topology.ComponentTiDB:
			*items = append(*items, store.InstanceItem{
				Instance:     fmt.Sprintf("%s:%d", com.IP, com.StatusPort),
				InstanceType: topology.ComponentTiDB,
				TimestampSec: uint64(now),
			})
		case topology.ComponentTiKV:
			*items = append(*items, store.InstanceItem{
				Instance:     fmt.Sprintf("%s:%d", com.IP, com.Port),
				InstanceType: topology.ComponentTiKV,
				TimestampSec: uint64(now),
			})
		}
	}
	return tmc.store.Instances(*items)
}
