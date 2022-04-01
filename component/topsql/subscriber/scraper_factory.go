package subscriber

import (
	"context"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
)

type ScraperFactory interface {
	NewScraper(ctx context.Context, component topology.Component) Scraper
	UpdateConfig(variable pdvariable.PDVariable, config *config.Config)
	IsEnabled() bool
}

type Scraper interface {
	Run()
	IsDown() bool
	Close()
}
