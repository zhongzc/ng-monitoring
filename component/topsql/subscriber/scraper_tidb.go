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

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

type TiDBScraper struct {
	component topology.Component
	store     store.Store
}

func NewTiDBScraper(component topology.Component, store store.Store) *TiDBScraper {
	return &TiDBScraper{component: component, store: store}
}

var _ scraper.Handler = &TiDBScraper{}
var _ scraper.Client = &TiDBScraper{}

func (ts *TiDBScraper) Connect(ctx context.Context, tlsConfig *tls.Config) (scraper.Channel, error) {
	conn, err := utils.DialGRPC(ctx, tlsConfig, ts.address())
	if err != nil {
		return nil, err
	}
	return NewTiDBChannel(ctx, conn)
}

func (ts *TiDBScraper) Handle(value interface{}) error {
	if value == nil {
		return errors.New("unexpected nil record")
	}

	record, ok := value.(*tipb.TopSQLSubResponse)
	if !ok {
		return errors.New("unexpected record type: " + reflect.TypeOf(value).String())
	}

	if record == nil {
		return errors.New("unexpected nil record")
	}

	if r := record.GetRecord(); r != nil {
		err := ts.store.TopSQLRecord(ts.address(), topology.ComponentTiDB, r)
		if err != nil {
			return errors.New("failed to store Top SQL record: " + err.Error())
		}
		return nil
	}

	if meta := record.GetSqlMeta(); meta != nil {
		err := ts.store.SQLMeta(meta)
		if err != nil {
			return errors.New("failed to store SQL meta: " + err.Error())
		}
		return nil
	}

	if meta := record.GetPlanMeta(); meta != nil {
		err := ts.store.PlanMeta(meta)
		if err != nil {
			return errors.New("failed to store plan meta: " + err.Error())
		}
	}
	return nil
}

func (ts *TiDBScraper) address() string {
	return fmt.Sprintf("%s:%d", ts.component.IP, ts.component.StatusPort)
}

type TiDBChannel struct {
	conn   *grpc.ClientConn
	client tipb.TopSQLPubSubClient
	stream tipb.TopSQLPubSub_SubscribeClient
}

func NewTiDBChannel(ctx context.Context, conn *grpc.ClientConn) (*TiDBChannel, error) {
	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &tipb.TopSQLSubRequest{})
	if err != nil {
		return nil, err
	}
	return &TiDBChannel{conn: conn, client: client, stream: stream}, nil
}

var _ scraper.Channel = &TiDBChannel{}

func (c *TiDBChannel) BlockingReceive() (interface{}, error) {
	if c.stream == nil {
		return nil, errors.New("stream is not connected")
	}

	return c.stream.Recv()
}

func (c *TiDBChannel) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.client = nil
	c.stream = nil
}
