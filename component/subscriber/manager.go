package subscriber

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
)

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	enabled    bool
	components []topology.Component

	scrapers map[topology.Component]Scraper

	topoSubscriber topology.Subscriber
	cfgSubscriber  config.Subscriber
	varSubscriber  pdvariable.Subscriber

	subscribeController SubscribeController
}

func NewManager(
	ctx context.Context,
	wg *sync.WaitGroup,
	varSubscriber pdvariable.Subscriber,
	topoSubscriber topology.Subscriber,
	cfgSubscriber config.Subscriber,
	subscribeController SubscribeController,
) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,

		scrapers: make(map[topology.Component]Scraper),

		varSubscriber:  varSubscriber,
		topoSubscriber: topoSubscriber,
		cfgSubscriber:  cfgSubscriber,

		subscribeController: subscribeController,
	}
}

func (m *Manager) Run() {
	defer func() {
		for _, v := range m.scrapers {
			v.Close()
		}
		m.scrapers = nil
	}()

	for {
		select {
		case getCfg := <-m.cfgSubscriber:
			m.subscribeController.UpdateConfig(getCfg())
		case getVars := <-m.varSubscriber:
			m.subscribeController.UpdatePDVariable(getVars())
		case getComs := <-m.topoSubscriber:
			m.components = getComs
			m.subscribeController.UpdateTopology(getComs)
		case <-m.ctx.Done():
			return
		}

		if m.subscribeController.IsEnabled() && !m.enabled {
			log.Info(fmt.Sprintf("%s is enabled", m.subscribeController.Name()))
		}
		if !m.subscribeController.IsEnabled() && m.enabled {
			log.Info(fmt.Sprintf("%s is disabled", m.subscribeController.Name()))
		}

		m.enabled = m.subscribeController.IsEnabled()

		if m.enabled {
			m.updateScrapers()
		} else {
			m.clearScrapers()
		}
	}
}

func (m *Manager) Close() {
	m.cancel()
}

func (m *Manager) updateScrapers() {
	// clean up closed scrapers
	for component, scraper := range m.scrapers {
		if scraper.IsDown() {
			scraper.Close()
			delete(m.scrapers, component)
		}
	}

	in, out := m.getTopoChange()

	// clean up stale scrapers
	for i := range out {
		m.scrapers[out[i]].Close()
		delete(m.scrapers, out[i])
	}

	// set up incoming scrapers
	for i := range in {
		scraper := m.subscribeController.NewScraper(m.ctx, in[i])
		m.scrapers[in[i]] = scraper

		m.wg.Add(1)
		go utils.GoWithRecovery(func() {
			defer m.wg.Done()
			scraper.Run()
		}, nil)
	}
}

func (m *Manager) getTopoChange() (in, out []topology.Component) {
	curMap := make(map[topology.Component]struct{})

	for i := range m.components {
		component := m.components[i]
		switch component.Name {
		case topology.ComponentTiDB:
		case topology.ComponentTiKV:
		default:
			continue
		}

		curMap[component] = struct{}{}
		if _, contains := m.scrapers[component]; !contains {
			in = append(in, component)
		}
	}

	for c := range m.scrapers {
		if _, contains := curMap[c]; !contains {
			out = append(out, c)
		}
	}

	return
}

func (m *Manager) clearScrapers() {
	for component, scraper := range m.scrapers {
		scraper.Close()
		delete(m.scrapers, component)
	}
}
