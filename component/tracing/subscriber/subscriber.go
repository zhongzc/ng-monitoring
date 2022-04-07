package subscriber

import (
	"context"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/tracing/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
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

func (sc *SubscriberController) NewScraper(ctx context.Context, component topology.Component) subscriber.Scraper {
	//return NewScraper(ctx, component, sc.store, sc.config.Security.GetTLSConfig())
	return nil
}

func (sc *SubscriberController) Name() string {
	return "Tracing"
}

func (sc *SubscriberController) IsEnabled() bool {
	return true
}

func (sc *SubscriberController) UpdatePDVariable(variable pdvariable.PDVariable) {
	sc.variable = &variable
}

func (sc *SubscriberController) UpdateConfig(cfg config.Config) {
	sc.config = &cfg
}

func (sc *SubscriberController) UpdateTopology(components []topology.Component) {
	sc.components = components
}
