package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/goto/salt/log"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	slackapi "github.com/slack-go/slack"
	"google.golang.org/grpc"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/event/moderator"
	jHandler "github.com/goto/optimus/core/job/handler/v1beta1"
	jResolver "github.com/goto/optimus/core/job/resolver"
	jService "github.com/goto/optimus/core/job/service"
	rModel "github.com/goto/optimus/core/resource"
	rHandler "github.com/goto/optimus/core/resource/handler/v1beta1"
	rService "github.com/goto/optimus/core/resource/service"
	schedulerHandler "github.com/goto/optimus/core/scheduler/handler/v1beta1"
	schedulerResolver "github.com/goto/optimus/core/scheduler/resolver"
	schedulerService "github.com/goto/optimus/core/scheduler/service"
	tHandler "github.com/goto/optimus/core/tenant/handler/v1beta1"
	tService "github.com/goto/optimus/core/tenant/service"
	"github.com/goto/optimus/ext/notify/alertmanager"
	"github.com/goto/optimus/ext/notify/pagerduty"
	"github.com/goto/optimus/ext/notify/slack"
	"github.com/goto/optimus/ext/notify/webhook"
	bqStore "github.com/goto/optimus/ext/store/bigquery"
	mcStore "github.com/goto/optimus/ext/store/maxcompute"
	"github.com/goto/optimus/ext/transport/kafka"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/store/postgres"
	"github.com/goto/optimus/internal/store/postgres/alerts"
	jRepo "github.com/goto/optimus/internal/store/postgres/job"
	"github.com/goto/optimus/internal/store/postgres/resource"
	schedulerRepo "github.com/goto/optimus/internal/store/postgres/scheduler"
	"github.com/goto/optimus/internal/store/postgres/sync"
	"github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/internal/telemetry"
	"github.com/goto/optimus/plugin"
	upstreamidentifier "github.com/goto/optimus/plugin/upstream_identifier"
	"github.com/goto/optimus/plugin/upstream_identifier/evaluator"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	oHandler "github.com/goto/optimus/server/handler/v1beta1"
)

const keyLength = 32

type setupFn func() error

type OptimusServer struct {
	conf   *config.ServerConfig
	logger log.Logger

	dbPool *pgxpool.Pool
	key    *[keyLength]byte

	grpcAddr   string
	httpAddr   string
	grpcServer *grpc.Server
	httpServer *http.Server

	pluginStore *plugin.Store
	cleanupFn   []func()

	eventHandler moderator.Handler
}

func New(conf *config.ServerConfig) (*OptimusServer, error) {
	addr := fmt.Sprintf(":%d", conf.Serve.PortGRPC)
	httpAddr := fmt.Sprintf(":%d", conf.Serve.Port)
	server := &OptimusServer{
		conf:     conf,
		grpcAddr: addr,
		httpAddr: httpAddr,
		logger:   NewLogger(conf.Log.Level.String()),
	}

	if err := checkRequiredConfigs(conf.Serve); err != nil {
		return server, err
	}

	setupFns := []setupFn{
		server.setupPublisher,
		server.setupPlugins,
		server.setupTelemetry,
		server.setupAppKey,
		server.setupDB,
		server.setupGRPCServer,
		server.setupHandlers,
		server.setupMonitoring,
		server.setupHTTPProxy,
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return server, err
		}
	}

	server.logger.Info("Starting Optimus", "version", config.BuildVersion)
	server.startListening()

	return server, nil
}

func (s *OptimusServer) setupPublisher() error {
	if s.conf.Publisher == nil {
		s.eventHandler = moderator.NoOpHandler{}
		return nil
	}

	ch := make(chan []byte, s.conf.Publisher.Buffer)

	var worker *moderator.Worker

	switch s.conf.Publisher.Type {
	case "kafka":
		var kafkaConfig config.PublisherKafkaConfig
		if err := mapstructure.Decode(s.conf.Publisher.Config, &kafkaConfig); err != nil {
			return err
		}

		writer := kafka.NewWriter(kafkaConfig.BrokerURLs, kafkaConfig.Topic, s.logger)
		interval := time.Second * time.Duration(kafkaConfig.BatchIntervalSecond)
		worker = moderator.NewWorker(ch, writer, interval, s.logger)
	default:
		return fmt.Errorf("publisher with type [%s] is not recognized", s.conf.Publisher.Type)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go worker.Run(ctx)

	s.cleanupFn = append(s.cleanupFn, func() {
		cancel()

		if err := worker.Close(); err != nil {
			s.logger.Error("error closing publishing worker: %v", err)
		}
	})

	s.eventHandler = moderator.NewEventHandler(ch, s.logger)
	return nil
}

func (s *OptimusServer) setupPlugins() error {
	store, err := plugin.LoadPluginToStore(s.logger, s.conf.Plugins.Location)
	s.pluginStore = store
	return err
}

func (s *OptimusServer) setupTelemetry() error {
	teleShutdown, err := telemetry.Init(s.logger, s.conf.Telemetry)
	if err != nil {
		return err
	}

	s.cleanupFn = append(s.cleanupFn, teleShutdown)
	return nil
}

func (s *OptimusServer) setupAppKey() error {
	var err error
	s.key, err = applicationKeyFromString(s.conf.Serve.AppKey)
	if err != nil {
		return err
	}

	return nil
}

func applicationKeyFromString(appKey string) (*[keyLength]byte, error) {
	if len(appKey) < keyLength {
		return nil, errors.InvalidArgument("application_key", "application key should be 32 chars in length")
	}

	var key [keyLength]byte
	_, err := io.ReadFull(bytes.NewBufferString(appKey), key[:])
	return &key, err
}

func (s *OptimusServer) setupDB() error {
	err := postgres.Migrate(s.conf.Serve.DB.DSN)
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	s.dbPool, err = postgres.Open(s.conf.Serve.DB)
	if err != nil {
		return fmt.Errorf("postgres.Open: %w", err)
	}

	return nil
}

func (s *OptimusServer) setupGRPCServer() error {
	var err error
	s.grpcServer, err = setupGRPCServer(s.logger)
	return err
}

func (s *OptimusServer) setupMonitoring() error {
	grpc_prometheus.Register(s.grpcServer)
	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(prometheus.DefBuckets))
	return nil
}

func (s *OptimusServer) setupHTTPProxy() error {
	srv, cleanup, err := prepareHTTPProxy(s.httpAddr, s.grpcAddr)
	s.httpServer = srv
	s.cleanupFn = append(s.cleanupFn, cleanup)
	return err
}

func (s *OptimusServer) startListening() {
	// run our server in a goroutine so that it doesn't block to wait for termination requests
	go func() {
		s.logger.Info("Listening for GRPC at", "address", s.grpcAddr)
		lis, err := net.Listen("tcp", s.grpcAddr)
		if err != nil {
			s.logger.Fatal("failed to listen: %v", err)
		}

		if err = s.grpcServer.Serve(lis); err != nil {
			s.logger.Fatal("failed to serve: %v", err)
		}
	}()
	go func() {
		s.logger.Info("Listening at", "address", s.httpAddr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				s.logger.Fatal("server error", "error", err)
			}
		}
	}()
}

func (s *OptimusServer) Shutdown() {
	s.logger.Warn("Shutting down server")
	if s.httpServer != nil {
		// Create a deadline to wait for server
		ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
		defer cancelProxy()

		if err := s.httpServer.Shutdown(ctxProxy); err != nil {
			s.logger.Error("Error in proxy shutdown", err)
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	for _, fn := range s.cleanupFn {
		fn() // Todo: log all the errors from cleanup before exit
	}

	if s.dbPool != nil {
		s.dbPool.Close()
	}

	s.logger.Info("Server shutdown complete")
}

func (s *OptimusServer) setupHandlers() error {
	// Tenant Bounded Context Setup
	tProjectRepo := tenant.NewProjectRepository(s.dbPool)
	tNamespaceRepo := tenant.NewNamespaceRepository(s.dbPool)
	tSecretRepo := tenant.NewSecretRepository(s.dbPool)
	presetRepo := tenant.NewPresetRepository(s.dbPool)

	tProjectService := tService.NewProjectService(tProjectRepo, presetRepo)
	tNamespaceService := tService.NewNamespaceService(tNamespaceRepo)
	tSecretService := tService.NewSecretService(s.key, tSecretRepo, s.logger)
	tenantService := tService.NewTenantService(tProjectService, tNamespaceService, tSecretService, s.logger)

	// Scheduler bounded context
	jobRunRepo := schedulerRepo.NewJobRunRepository(s.dbPool)
	operatorRunRepository := schedulerRepo.NewOperatorRunRepository(s.dbPool)
	slaRepository := schedulerRepo.NewSLARepository(s.dbPool)
	jobProviderRepo := schedulerRepo.NewJobProviderRepository(s.dbPool)

	notificationContext, cancelNotifiers := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, cancelNotifiers)

	notifierChanels := map[string]schedulerService.Notifier{}
	if s.conf.Alerting.EnableSlack {
		notifierChanels["slack"] = slack.NewNotifier(notificationContext, slackapi.APIURL,
			slack.DefaultEventBatchInterval,
			func(err error) {
				s.logger.Error("slack error accumulator", "error", err)
			},
		)
	}
	if s.conf.Alerting.EnablePagerDuty {
		notifierChanels["pagerduty"] = pagerduty.NewNotifier(
			notificationContext,
			pagerduty.DefaultEventBatchInterval,
			func(err error) {
				s.logger.Error("pagerduty error accumulator", "error", err)
			},
			new(pagerduty.PagerDutyServiceImpl),
		)
	}

	webhookNotifier := webhook.NewNotifier(
		notificationContext,
		webhook.DefaultEventBatchInterval,
		func(err error) {
			s.logger.Error("webhook error accumulator : " + err.Error())
		},
	)

	alertsLogRepo := alerts.NewAlertRepository(s.dbPool)

	alertsHandler := new(alertmanager.AlertManager)
	if s.conf.Alerting.EventManager.Enabled {
		alertsHandler = alertmanager.New(
			notificationContext,
			s.logger,
			s.conf.Alerting.EventManager.Host,
			s.conf.Alerting.EventManager.Endpoint,
			s.conf.Alerting.Dashboard,
			s.conf.Alerting.DataConsole,
			alertsLogRepo,
		)
	}

	newEngine := compiler.NewEngine()

	newPriorityResolver := schedulerResolver.NewSimpleResolver()
	assetCompiler := schedulerService.NewJobAssetsCompiler(newEngine, s.logger)
	jobInputCompiler := schedulerService.NewJobInputCompiler(tenantService, newEngine, assetCompiler, s.logger)
	eventsService := schedulerService.NewEventsService(s.logger, jobProviderRepo, tenantService, notifierChanels, webhookNotifier, newEngine, alertsHandler)
	newScheduler, err := NewScheduler(s.logger, s.conf, s.pluginStore, tProjectService, tSecretService)
	if err != nil {
		return err
	}

	replayRepository := schedulerRepo.NewReplayRepository(s.dbPool)
	replayWorker := schedulerService.NewReplayWorker(s.logger, replayRepository, jobProviderRepo, newScheduler, s.conf.Replay, alertsHandler, tenantService)

	replayContext, closeReplayScan := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, closeReplayScan)
	go replayWorker.ScanReplayRequest(replayContext)

	replayValidator := schedulerService.NewValidator(replayRepository, newScheduler, jobProviderRepo)
	replayService := schedulerService.NewReplayService(
		replayRepository, jobProviderRepo, tenantService,
		replayValidator, replayWorker, newScheduler,
		s.logger, s.conf.Replay.PluginExecutionProjectConfigNames, alertsHandler,
	)

	autoSLADurationEstimatorService := schedulerService.NewDurationEstimatorService(s.logger, jobRunRepo,
		s.conf.Alerting.AutoSLABreachConfig.LastNRuns, s.conf.Alerting.AutoSLABreachConfig.Percentile,
		s.conf.Alerting.AutoSLABreachConfig.PaddingPercentage, s.conf.Alerting.AutoSLABreachConfig.MinPaddingMinutes,
		s.conf.Alerting.AutoSLABreachConfig.MaxPaddingMinutes)

	newJobRunService := schedulerService.NewJobRunService(
		s.logger, jobProviderRepo, jobRunRepo, replayRepository, operatorRunRepository, slaRepository,
		newScheduler, newPriorityResolver, jobInputCompiler, s.eventHandler, tProjectService,
		s.conf.Features, autoSLADurationEstimatorService,
	)

	newSchedulerService := schedulerService.NewSchedulerService(newScheduler)

	// Plugin
	upstreamIdentifierFactory, _ := upstreamidentifier.NewUpstreamIdentifierFactory(s.logger)
	evaluatorFactory, _ := evaluator.NewEvaluatorFactory(s.logger)
	pluginService, _ := plugin.NewPluginService(s.logger, s.pluginStore, upstreamIdentifierFactory, evaluatorFactory)
	syncStatusRepository := sync.NewStatusSyncRepository(s.dbPool)

	maxSyncDelayTolerance := time.Duration(s.conf.ExternalTables.MaxSyncDelayTolerance) * time.Hour
	syncer := mcStore.NewSyncer(s.logger, tenantService, tenantService, syncStatusRepository,
		s.conf.ExternalTables.MaxFileSizeSupported, s.conf.ExternalTables.DriveFileCleanupSizeLimit, maxSyncDelayTolerance)

	// Resource Bounded Context - requirements
	resourceRepository := resource.NewRepository(s.dbPool)

	backupRepository := resource.NewBackupRepository(s.dbPool)
	resourceManager := rService.NewResourceManager(resourceRepository, s.logger)
	secondaryResourceService := rService.NewResourceService(s.logger, resourceRepository, nil, resourceManager, s.eventHandler, nil, alertsHandler, tenantService, newEngine, syncer, syncStatusRepository) // note: job service can be nil

	// Job Bounded Context Setup
	jJobRepo := jRepo.NewJobRepository(s.dbPool)
	jExternalUpstreamResolver, _ := jResolver.NewExternalUpstreamResolver(s.conf.ResourceManagers)
	jInternalUpstreamResolver := jResolver.NewInternalUpstreamResolver(jJobRepo)
	jUpstreamResolvers, err := jResolver.NewThirdPartyUpstreamResolvers(s.logger, tenantService, s.conf.UpstreamResolvers...)
	if err != nil {
		return err
	}
	jUpstreamResolver := jResolver.NewUpstreamResolver(jJobRepo, jExternalUpstreamResolver, jInternalUpstreamResolver, jUpstreamResolvers...)
	jJobService := jService.NewJobService(
		jJobRepo, jJobRepo, jJobRepo,
		pluginService, jUpstreamResolver, tenantService,
		s.eventHandler, s.logger, newJobRunService, newEngine,
		jobInputCompiler, secondaryResourceService, alertsHandler,
	)

	jobWorker := jService.NewJobWorker(s.logger, jJobRepo, newJobRunService)
	jobWorkerCtx, closeJobWorker := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, closeJobWorker)
	if s.conf.JobSyncIntervalMinutes > 0 {
		go jobWorker.SyncJobStatus(jobWorkerCtx, s.conf.JobSyncIntervalMinutes)
	}

	jchangeLogService := jService.NewChangeLogService(jJobRepo)

	lineageBuilder := schedulerResolver.NewLineageResolver(jobProviderRepo, jobProviderRepo, newJobRunService, tProjectService, s.logger)
	// TODO: since no service consume this yet, we can wait to initialize this when needed
	jobLineageService := schedulerService.NewJobLineageService(s.logger, lineageBuilder)

	// SLA Predictor Service
	newDurationEstimatorService := schedulerService.NewDurationEstimatorService(s.logger, jobRunRepo,
		s.conf.Alerting.PotentialSLABreachConfig.DurationEstimatorConfig.LastNRuns, s.conf.Alerting.PotentialSLABreachConfig.DurationEstimatorConfig.Percentile,
		s.conf.Alerting.PotentialSLABreachConfig.DurationEstimatorConfig.PaddingPercentage, s.conf.Alerting.PotentialSLABreachConfig.DurationEstimatorConfig.MinPaddingMinutes,
		s.conf.Alerting.PotentialSLABreachConfig.DurationEstimatorConfig.MaxPaddingMinutes)

	newJobSLAPredictorService := schedulerService.NewJobSLAPredictorService(s.logger, s.conf.Alerting.PotentialSLABreachConfig, slaRepository, jobLineageService, newDurationEstimatorService, jobProviderRepo, alertsHandler, tenantService)

	// Resource Bounded Context
	primaryResourceService := rService.NewResourceService(s.logger, resourceRepository, jJobService, resourceManager, s.eventHandler, jJobService, alertsHandler, tenantService, newEngine, syncer, syncStatusRepository)
	backupService := rService.NewBackupService(backupRepository, resourceRepository, resourceManager, s.logger)
	resourceChangeLogService := rService.NewChangelogService(s.logger, resourceRepository)

	// Register datastore
	bqClientProvider := bqStore.NewClientProvider()
	bigqueryStore := bqStore.NewBigqueryDataStore(tenantService, bqClientProvider)
	resourceManager.RegisterDatastore(rModel.Bigquery, bigqueryStore)

	mcClientProvider := mcStore.NewClientProvider()
	maxComputeStore := mcStore.NewMaxComputeDataStore(s.logger, tenantService, mcClientProvider, tenantService,
		syncStatusRepository, s.conf.ExternalTables.MaxFileSizeSupported, s.conf.ExternalTables.DriveFileCleanupSizeLimit, maxSyncDelayTolerance)
	resourceManager.RegisterDatastore(rModel.MaxCompute, maxComputeStore)

	slaWorkerCtx, closeSLAWorker := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, closeSLAWorker)
	slaWorker := schedulerService.NewSLAWorker(s.logger, alertsHandler, slaRepository, jobProviderRepo, jobRunRepo, operatorRunRepository, tenantService, newScheduler)
	slaWorker.ScheduleSLAHandling(slaWorkerCtx,
		time.Minute*time.Duration(s.conf.SLAConfig.WorkerIntervalMinutes),
		time.Minute*time.Duration(s.conf.SLAConfig.LockDurationMinutes))

	resourceWorkerCtx, closeResourceWorker := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, closeResourceWorker)

	resourceWorker := rService.NewResourceWorker(s.logger, resourceRepository, syncer, resourceManager, primaryResourceService)
	if s.conf.ExternalTables.AccessIssuesRetryInterval > 0 {
		go resourceWorker.RetrySheetsAccessIssues(resourceWorkerCtx, s.conf.ExternalTables.AccessIssuesRetryInterval)
	}
	if s.conf.ExternalTables.SourceSyncInterval > 0 {
		go resourceWorker.SyncExternalSheets(resourceWorkerCtx, s.conf.ExternalTables.SourceSyncInterval)
	}

	// Tenant Handlers
	pb.RegisterSecretServiceServer(s.grpcServer, tHandler.NewSecretsHandler(s.logger, tSecretService))
	pb.RegisterProjectServiceServer(s.grpcServer, tHandler.NewProjectHandler(s.logger, tProjectService))
	pb.RegisterNamespaceServiceServer(s.grpcServer, tHandler.NewNamespaceHandler(s.logger, tNamespaceService))

	// Resource Handler
	pb.RegisterResourceServiceServer(s.grpcServer, rHandler.NewResourceHandler(s.logger, primaryResourceService, resourceChangeLogService))

	sensorService := schedulerService.NewSensorService(s.logger, s.conf.UpstreamResolvers...)
	pb.RegisterJobRunServiceServer(s.grpcServer, schedulerHandler.NewJobRunHandler(s.logger, newJobRunService, eventsService, newSchedulerService, jobLineageService, newJobSLAPredictorService, sensorService))

	// backup service
	pb.RegisterBackupServiceServer(s.grpcServer, rHandler.NewBackupHandler(s.logger, backupService))

	// version service
	pb.RegisterRuntimeServiceServer(s.grpcServer, oHandler.NewVersionHandler(s.logger, config.BuildVersion))

	// Core Job Handler
	pb.RegisterJobSpecificationServiceServer(s.grpcServer, jHandler.NewJobHandler(jJobService, jchangeLogService, s.logger))

	pb.RegisterReplayServiceServer(s.grpcServer, schedulerHandler.NewReplayHandler(s.logger, replayService))

	s.cleanupFn = append(s.cleanupFn, func() {
		err = eventsService.Close()
		if err != nil {
			s.logger.Error("Error while closing event service: %s", err)
		}
	})

	return nil
}
