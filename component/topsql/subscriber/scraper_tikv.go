package subscriber

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"

	"github.com/pingcap/ng-monitoring/component/subscriber/scraper"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"google.golang.org/grpc"
)

type TiKVScraper struct {
	component topology.Component
	store     store.Store
}

func NewTiKVScraper(component topology.Component, store store.Store) *TiKVScraper {
	return &TiKVScraper{component: component, store: store}
}

var _ scraper.Handler = &TiKVScraper{}
var _ scraper.Client = &TiKVScraper{}

func (ts *TiKVScraper) Connect(ctx context.Context, tlsConfig *tls.Config) (scraper.Channel, error) {
	conn, err := utils.DialGRPC(ctx, tlsConfig, ts.address())
	if err != nil {
		return nil, err
	}
	return NewTiKVChannel(ctx, conn)
}

func (ts *TiKVScraper) Handle(value interface{}) error {
	if value == nil {
		return errors.New("unexpected nil record")
	}

	record, ok := value.(*rua.ResourceUsageRecord)
	if !ok {
		return errors.New("unexpected record type: " + reflect.TypeOf(value).String())
	}

	if record == nil {
		return errors.New("unexpected nil record")
	}

	err := ts.store.ResourceMeteringRecord(ts.address(), topology.ComponentTiKV, record)
	if err != nil {
		return errors.New("failed to store resource metering record: " + err.Error())
	}
	return nil
}

func (ts *TiKVScraper) address() string {
	return fmt.Sprintf("%s:%d", ts.component.IP, ts.component.Port)
}

type TiKVChannel struct {
	conn   *grpc.ClientConn
	client rua.ResourceMeteringPubSubClient
	stream rua.ResourceMeteringPubSub_SubscribeClient
}

func NewTiKVChannel(ctx context.Context, conn *grpc.ClientConn) (*TiKVChannel, error) {
	client := rua.NewResourceMeteringPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &rua.ResourceMeteringRequest{})
	if err != nil {
		return nil, err
	}
	return &TiKVChannel{conn: conn, client: client, stream: stream}, nil
}

var _ scraper.Channel = &TiKVChannel{}

func (c *TiKVChannel) BlockingReceive() (interface{}, error) {
	if c.stream == nil {
		return nil, errors.New("stream is not connected")
	}

	return c.stream.Recv()
}

func (c *TiKVChannel) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.client = nil
	c.stream = nil
}
