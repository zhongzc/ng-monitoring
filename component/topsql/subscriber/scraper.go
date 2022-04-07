package subscriber

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/ng-monitoring/component/subscriber/scraper"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
)

func NewScraper(
	ctx context.Context,
	component topology.Component,
	store store.Store,
	tlsConfig *tls.Config,
) *scraper.Scraper {
	switch component.Name {
	case topology.ComponentTiDB:
		s := NewTiDBScraper(component, store)
		return scraper.NewScraper("Top SQL", component, ctx, tlsConfig, s, s)
	case topology.ComponentTiKV:
		s := NewTiKVScraper(component, store)
		return scraper.NewScraper("Top SQL", component, ctx, tlsConfig, s, s)
	default:
		return nil
	}
}
