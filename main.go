package main

import (
	"context"
	"fmt"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	stdlog "log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof"
	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/document"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/ng-monitoring/service"
	"github.com/pingcap/ng-monitoring/utils/printer"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	nmVersion          = "version"
	nmAddr             = "address"
	nmPdEndpoints      = "pd.endpoints"
	nmLogPath          = "log.path"
	nmStoragePath      = "storage.path"
	nmConfig           = "config"
	nmAdvertiseAddress = "advertise-address"
)

var (
	version           = pflag.BoolP(nmVersion, "V", false, "print version information and exit")
	listenAddr        = pflag.String(nmAddr, "", "TCP address to listen for http connections")
	pdEndpoints       = pflag.StringSlice(nmPdEndpoints, nil, "Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. --pd.endpoints 10.0.0.1:2379,10.0.0.2:2379")
	logPath           = pflag.String(nmLogPath, "", "Log path of ng monitoring server")
	storagePath       = pflag.String(nmStoragePath, "", "Storage path of ng monitoring server")
	configPath        = pflag.String(nmConfig, "", "config file path")
	advertiseAddress  = pflag.String(nmAdvertiseAddress, "", "ngm server advertise IP:PORT")
	mockTargets       = pflag.StringSlice("mock-targets", nil, "mock targets")
	mockWithDashboard = pflag.Bool("mock-with-dashboard", false, "mock with dashboard")
	mocker            = pflag.Bool("mocker", false, "run as mocker")
)

func main() {
	// There are dependencies that use `flag`.
	// For isolation and avoiding conflict, we use another command line parser package `pflag`.
	pflag.Parse()

	if *mocker {
		runMockers(*mockTargets)
		procutil.WaitForSigterm()
		return
	}

	if *version {
		fmt.Println(printer.GetNGMInfo())
		return
	}

	cfg, err := config.InitConfig(*configPath, overrideConfig)
	if err != nil {
		stdlog.Fatalf("Failed to initialize config, err: %s", err.Error())
	}

	cfg.Log.InitDefaultLogger()
	printer.PrintNGMInfo()
	log.Info("config", zap.Any("config", cfg))

	mustCreateDirs(cfg)

	database.Init(cfg)
	defer database.Stop()

	err = config.LoadConfigFromStorage(document.Get)
	if err != nil {
		stdlog.Fatalf("Failed to load config from storage, err: %s", err.Error())
	}

	if len(*mockTargets) > 0 {
		targets := make([]topology.Component, 0, len(*mockTargets))
		for _, target := range *mockTargets {
			ss := strings.Split(target, ":")
			ip := ss[0]
			port, _ := strconv.ParseUint(ss[1], 10, 64)
			targets = append(targets, topology.Component{
				Name:       "tidb",
				IP:         ip,
				Port:       uint(port),
				StatusPort: uint(port),
			})
		}
		topology.InitForTest(targets)
		pdvariable.InitForTest(pdvariable.PDVariable{EnableTopSQL: true})

		if *mockWithDashboard {
			do := domain.NewDomain()
			defer do.Close()
			syncer := topology.NewTopologySyncer(do)
			syncer.Start()
			defer syncer.Stop()
		}
	} else {
		do := domain.NewDomain()
		defer do.Close()

		err = topology.Init(do)
		if err != nil {
			log.Fatal("Failed to initialize topology", zap.Error(err))
		}
		defer topology.Stop()

		pdvariable.Init(do)
		defer pdvariable.Stop()
	}

	err = topsql.Init(config.Subscribe(), document.Get(), timeseries.InsertHandler, timeseries.SelectHandler, topology.Subscribe(), pdvariable.Subscribe())
	if err != nil {
		log.Fatal("Failed to initialize topsql", zap.Error(err))
	}
	defer topsql.Stop()

	err = conprof.Init(document.Get(), topology.Subscribe())
	if err != nil {
		log.Fatal("Failed to initialize continuous profiling", zap.Error(err))
	}
	defer conprof.Stop()

	service.Init(cfg)
	defer service.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go config.ReloadRoutine(ctx, *configPath, cfg)
	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}

func overrideConfig(config *config.Config) {
	pflag.Visit(func(f *pflag.Flag) {
		switch f.Name {
		case nmAddr:
			config.Address = *listenAddr
		case nmPdEndpoints:
			config.PD.Endpoints = *pdEndpoints
		case nmLogPath:
			config.Log.Path = *logPath
		case nmStoragePath:
			config.Storage.Path = *storagePath
		case nmAdvertiseAddress:
			config.AdvertiseAddress = *advertiseAddress
		}
	})
}

func mustCreateDirs(config *config.Config) {
	if config.Log.Path != "" {
		if err := os.MkdirAll(config.Log.Path, os.ModePerm); err != nil {
			log.Fatal("failed to init log path", zap.Error(err))
		}
	}

	if err := os.MkdirAll(config.Storage.Path, os.ModePerm); err != nil {
		log.Fatal("failed to init storage path", zap.Error(err))
	}
}

func runMockers(targets []string) {
	for _, target := range targets {
		port, _ := strconv.ParseUint(strings.Split(target, ":")[1], 10, 64)
		mockTargetInPort(uint(port))
	}
}

func mockTargetInPort(port uint) {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatal("listen failed", zap.Error(err), zap.Uint("server", port))
	}
	m := cmux.New(lis)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())
	grpcL := m.Match(cmux.Any())

	go func() {
		log.Info("start to serve conprof", zap.Uint("server", port))
		err := http.Serve(httpL, conprofHTTP1Route(port))
		if err != nil {
			log.Fatal("serve debug failed", zap.Error(err), zap.Uint("server", port))
		}
	}()
	go func() {
		log.Info("start to serve topsql", zap.Uint("server", port))
		err := topsqlGRPCServer(port).Serve(grpcL)
		if err != nil {
			log.Fatal("serve pubsub failed", zap.Error(err), zap.Uint("server", port))
		}
	}()
	go func() {
		log.Info("start to serve", zap.Uint("server", port))
		err = m.Serve()
		if err != nil {
			log.Fatal("serve failed", zap.Error(err))
		}
	}()
}

func topsqlGRPCServer(port uint) *grpc.Server {
	server := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    5 * time.Second,
		Timeout: 3 * time.Second,
	}))
	tipb.RegisterTopSQLPubSubServer(server, &topsqlPubSubService{port: port})
	return server
}

func conprofHTTP1Route(port uint) *http.ServeMux {
	router := http.NewServeMux()

	router.HandleFunc("/debug/pprof/mutex", func(writer http.ResponseWriter, request *http.Request) {
		var bytes []byte
		for i := 0; i < 100*1024; i++ {
			bytes = append(bytes, byte(rand.Intn(255)))
		}
		_, err := writer.Write(bytes)
		if err != nil {
			log.Warn("failed to send mutex", zap.Uint("server", port))
		}
		log.Info("sent mutex", zap.Uint("server", port))
	})
	router.HandleFunc("/debug/pprof/heap", func(writer http.ResponseWriter, request *http.Request) {
		var bytes []byte
		for i := 0; i < 400*1024; i++ {
			bytes = append(bytes, byte(rand.Intn(255)))
		}
		_, err := writer.Write(bytes)
		if err != nil {
			log.Warn("failed to send heap", zap.Uint("server", port))
		}
		log.Info("sent heap", zap.Uint("server", port))
	})
	router.HandleFunc("/debug/pprof/goroutine", func(writer http.ResponseWriter, request *http.Request) {
		var bytes []byte
		for i := 0; i < 100*1024; i++ {
			bytes = append(bytes, byte(rand.Intn(255)))
		}
		_, err := writer.Write(bytes)
		if err != nil {
			log.Warn("failed to send goroutine", zap.Uint("server", port))
		}
		log.Info("sent goroutine", zap.Uint("server", port))
	})
	router.HandleFunc("/debug/pprof/profile", func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(10 * time.Second)
		var bytes []byte
		for i := 0; i < 100*1024; i++ {
			bytes = append(bytes, byte(rand.Intn(255)))
		}
		_, err := writer.Write(bytes)
		if err != nil {
			log.Warn("failed to send profile", zap.Uint("server", port))
		}
		log.Info("sent profile", zap.Uint("server", port))
	})

	return router
}

type topsqlPubSubService struct {
	port uint
}

var _ tipb.TopSQLPubSubServer = &topsqlPubSubService{}

func (t *topsqlPubSubService) Subscribe(_ *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer) error {
	log.Info("receive a subscriber", zap.Uint("server", t.port))
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			log.Warn("subscriber is disconnected", zap.Uint("server", t.port))
			return nil
		case <-ticker.C:
			t.sendTopSQL(stream)
		}
	}
}

func (t *topsqlPubSubService) sendTopSQL(stream tipb.TopSQLPubSub_SubscribeServer) {
	log.Info("begin to send", zap.Uint("server", t.port))
	now := time.Now()
	for i := 0; i < 1000; i++ {
		meta := rand.Intn(2000)
		sqlDigest := fmt.Sprintf("mock_sql_digest_%d", meta)
		sqlText := fmt.Sprintf("mock_normalized_sql_%d", meta)
		planDigest := fmt.Sprintf("mock_plan_digest_%d", meta)
		planText := fmt.Sprintf("mock__normalized_plan_%d", meta)

		err := stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_SqlMeta{
			SqlMeta: &tipb.SQLMeta{
				SqlDigest:     []byte(sqlDigest),
				NormalizedSql: sqlText,
			},
		}})
		if err != nil {
			log.Warn("send sql meta failed", zap.Uint("server", t.port), zap.Time("now", now), zap.Error(err))
		}
		err = stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_PlanMeta{
			PlanMeta: &tipb.PlanMeta{
				PlanDigest:     []byte(planDigest),
				NormalizedPlan: planText,
			},
		}})
		if err != nil {
			log.Warn("send plan meta failed", zap.Uint("server", t.port), zap.Time("now", now), zap.Error(err))
		}

		nowSecs := now.Unix()
		var items []*tipb.TopSQLRecordItem
		for j := 60; j > 0; j-- {
			items = append(items, &tipb.TopSQLRecordItem{
				TimestampSec:      uint64(nowSecs) - uint64(j),
				CpuTimeMs:         uint32(rand.Int31n(100)),
				StmtExecCount:     uint64(rand.Int31n(100)),
				StmtDurationSumNs: uint64(rand.Int31n(1000000000)),
				StmtDurationCount: uint64(rand.Int31n(100)),
			})
		}
		err = stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_Record{
			Record: &tipb.TopSQLRecord{
				SqlDigest:  []byte(sqlDigest),
				PlanDigest: []byte(planDigest),
				Items:      items,
			},
		}})
		if err != nil {
			log.Warn("send record failed", zap.Uint("server", t.port), zap.Time("now", now), zap.Error(err))
		}
	}
	log.Info("all sent", zap.Uint("server", t.port), zap.Time("now", now))
}
