package query

import (
	"sync"

	"github.com/pingcap/ng-monitoring/component/tracing/db"
)

type TraceItemSlicePool struct {
	p sync.Pool
}

func (p *TraceItemSlicePool) Get() *[]*db.TraceItem {
	v := p.p.Get()
	if v == nil {
		return &[]*db.TraceItem{}
	}
	return v.(*[]*db.TraceItem)
}

func (p *TraceItemSlicePool) Put(v *[]*db.TraceItem) {
	*v = (*v)[:0]
	p.p.Put(v)
}
