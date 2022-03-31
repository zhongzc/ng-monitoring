package db

import "sync"

type MockDB struct {
	sync.Mutex
	items []*TraceItem
}

var _ DB = &MockDB{}

func (m *MockDB) Put(tasks []*WriteDBTask) error {
	m.Lock()
	defer m.Unlock()
	for _, task := range tasks {
		item := TraceItem(*task)
		m.items = append(m.items, &item)
	}
	return nil
}

func (m *MockDB) Get(traceID uint64, fill *[]*TraceItem) error {
	m.Lock()
	defer m.Unlock()
	for _, item := range m.items {
		if item.TraceID == traceID {
			*fill = append(*fill, item)
		}
	}
	return nil
}

func (m *MockDB) TakeAll() []*TraceItem {
	m.Lock()
	items := m.items
	m.items = nil
	m.Unlock()
	return items
}
