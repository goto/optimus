package service_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

func TestNotificationService(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	startDate, _ := time.Parse(time.RFC3339, "2022-03-20T02:00:00+00:00")
	jobName := scheduler.JobName("job1")
	t.Run("Push", func(t *testing.T) {
		t.Run("should give error if getJobDetails fails", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(nil, fmt.Errorf("some error"))
			defer jobRepo.AssertExpectations(t)

			notifyService := service.NewEventsService(logger, jobRepo, nil, nil, nil, nil, nil, nil)

			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.TaskStartEvent,
				Values:  map[string]any{},
			}
			err := notifyService.Push(ctx, event)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("should send notification to the appropriate channel for sla miss notify", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
				Task: &scheduler.Task{
					Name: "bq2bq",
				},
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Webhook: []scheduler.Webhook{
					{
						On: scheduler.EventCategorySLAMiss,
						Endpoints: []scheduler.WebhookEndPoint{
							{
								URL: "http://someDomain.com/endpoint",
								Headers: map[string]string{
									"header": "headerValue",
								},
							},
						},
					},
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategorySLAMiss,
						Channels: []string{"slack://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.SLAMissEvent,
				Values:  map[string]any{},
			}

			t.Run("should send  webhook notification", func(t *testing.T) {
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				plainSecret, _ := tenant.NewPlainTextSecret("NOTIFY_SLACK", "secretValue")
				plainSecrets := []*tenant.PlainTextSecret{plainSecret}
				tenantService := new(mockTenantService)
				tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
				defer tenantService.AssertExpectations(t)

				webhookChannel := new(mockWebhookChanel)
				webhookChannel.On("Trigger", scheduler.WebhookAttrs{
					Owner:    "jobOwnerName",
					JobEvent: event,
					Meta: &scheduler.JobRunMeta{
						Labels:         nil,
						DestinationURN: resource.ZeroURN(),
					},
					Route: "http://someDomain.com/endpoint",
					Headers: map[string]string{
						"header": "headerValue",
					},
				})

				templateCompiler := new(mockTemplateCompiler)
				secretContext := mock.Anything
				templateCompiler.On("Compile", mock.Anything, secretContext).Return(map[string]string{"header": "headerValue"}, nil)
				defer templateCompiler.AssertExpectations(t)

				notifyService := service.NewEventsService(logger, jobRepo, tenantService, nil, webhookChannel, nil, templateCompiler, nil)

				err := notifyService.Webhook(ctx, event)
				assert.Nil(t, err)
			})

			t.Run("should send event to alert manager", func(t *testing.T) {
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				alertManager := new(mockAlertManager)
				alertManager.On("Relay", &scheduler.AlertAttrs{
					Owner:         "jobOwnerName",
					JobURN:        job.URN(),
					Title:         "Optimus Job Alert",
					SchedulerHost: "localhost",
					Status:        scheduler.StatusFiring,
					JobEvent:      event,
				})
				tenantService := new(mockTenantService)
				tenantWithDetails, _ := tenant.NewTenantDetails(project, namespace, []*tenant.PlainTextSecret{})
				tenantService.On("GetDetails", ctx, tnnt).Return(tenantWithDetails, nil)
				defer tenantService.AssertExpectations(t)

				notifyService := service.NewEventsService(logger, jobRepo, tenantService, nil, nil, nil, nil, alertManager)

				err := notifyService.Relay(ctx, event)
				assert.Nil(t, err)
			})

			t.Run("should send slack notification", func(t *testing.T) {
				jobRepo := new(JobRepository)
				jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
				defer jobRepo.AssertExpectations(t)

				plainSecret, _ := tenant.NewPlainTextSecret("NOTIFY_SLACK", "secretValue")
				larkAppID, _ := tenant.NewPlainTextSecret(tenant.SecretLarkAppID, "secretValue1")
				larkAppIDSecret, _ := tenant.NewPlainTextSecret(tenant.SecretLarkAppSecret, "secretValue2")
				larkAppVerificationToken, _ := tenant.NewPlainTextSecret(tenant.SecretLarkVerificationToken, "secretValue3")
				plainSecrets := []*tenant.PlainTextSecret{plainSecret, larkAppID, larkAppIDSecret, larkAppVerificationToken}
				tenantService := new(mockTenantService)
				tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
				defer tenantService.AssertExpectations(t)

				notifyChanelSlack := new(mockNotificationChanel)
				notifyChanelSlack.On("Notify", ctx, scheduler.NotifyAttrs{
					Owner:    "jobOwnerName",
					JobEvent: event,
					Route:    "#chanel-name",
					Secret:   "secretValue",
				}).Return(nil)
				defer notifyChanelSlack.AssertExpectations(t)
				notifyChanelPager := new(mockNotificationChanel)
				defer notifyChanelPager.AssertExpectations(t)

				larkNotifyChannel := new(mockLarkNotificationChanel)

				larkNotifyChannel.On("Notify", ctx, scheduler.LarkNotifyAttrs{
					Owner:             "jobOwnerName",
					JobEvent:          event,
					Route:             "#chanel-name",
					AppID:             "secretValue1",
					AppSecret:         "secretValue2",
					VerificationToken: "secretValue3",
				}).Return(nil)

				defer larkNotifyChannel.AssertExpectations(t)

				notifierChannels := map[string]service.Notifier{
					"slack":     notifyChanelSlack,
					"pagerduty": notifyChanelPager,
				}

				notifyService := service.NewEventsService(logger, jobRepo, tenantService, notifierChannels, nil, larkNotifyChannel, nil, nil)

				err := notifyService.Push(ctx, event)
				assert.Nil(t, err)
			})
		})
		t.Run("should send notification to the appropriate channel for job fail", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategoryJobFailure,
						Channels: []string{"pagerduty://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			plainSecret, _ := tenant.NewPlainTextSecret("notify_chanel-name", "secretValue")
			plainSecrets := []*tenant.PlainTextSecret{plainSecret}
			tenantService := new(mockTenantService)
			tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
			defer tenantService.AssertExpectations(t)

			notifChanelSlack := new(mockNotificationChanel)
			defer notifChanelSlack.AssertExpectations(t)
			notifyChanelPager := new(mockNotificationChanel)
			notifyChanelPager.On("Notify", ctx, scheduler.NotifyAttrs{
				Owner:    "jobOwnerName",
				JobEvent: event,
				Route:    "#chanel-name",
				Secret:   "secretValue",
			}).Return(nil)
			defer notifyChanelPager.AssertExpectations(t)

			notifierChannels := map[string]service.Notifier{
				"slack":     notifChanelSlack,
				"pagerduty": notifyChanelPager,
			}

			notifyService := service.NewEventsService(logger, jobRepo, tenantService, notifierChannels, nil, nil, nil, nil)

			err := notifyService.Push(ctx, event)
			assert.Nil(t, err)
		})
		t.Run("should return error if notification to the appropriate channel for job_failure fails", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategoryJobFailure,
						Channels: []string{"pagerduty://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			plainSecret, _ := tenant.NewPlainTextSecret("notify_chanel-name", "secretValue")
			plainSecrets := []*tenant.PlainTextSecret{plainSecret}
			tenantService := new(mockTenantService)
			tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
			defer tenantService.AssertExpectations(t)

			notifyChanelSlack := new(mockNotificationChanel)
			defer notifyChanelSlack.AssertExpectations(t)
			notifyChanelPager := new(mockNotificationChanel)
			notifyChanelPager.On("Notify", ctx, scheduler.NotifyAttrs{
				Owner:    "jobOwnerName",
				JobEvent: event,
				Route:    "#chanel-name",
				Secret:   "secretValue",
			}).Return(fmt.Errorf("error in pagerduty push"))
			defer notifyChanelPager.AssertExpectations(t)

			notifierChannels := map[string]service.Notifier{
				"slack":     notifyChanelSlack,
				"pagerduty": notifyChanelPager,
			}

			notifyService := service.NewEventsService(logger, jobRepo, tenantService, notifierChannels, nil, nil, nil, nil)

			err := notifyService.Push(ctx, event)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "ErrorsInNotifyPush:\n notifyChannel.Notify: pagerduty://#chanel-name: error in pagerduty push")
		})
	})
}

// todo: this is added as Lark Notifer
type mockLarkNotificationChanel struct {
	io.Closer
	mock.Mock
}

func (m *mockLarkNotificationChanel) Notify(ctx context.Context, attr scheduler.LarkNotifyAttrs) error {
	args := m.Called(ctx, attr)
	return args.Error(0)
}

func (m *mockLarkNotificationChanel) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockNotificationChanel struct {
	io.Closer
	mock.Mock
}

func (m *mockNotificationChanel) Notify(ctx context.Context, attr scheduler.NotifyAttrs) error {
	args := m.Called(ctx, attr)
	return args.Error(0)
}

func (m *mockNotificationChanel) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockWebhookChanel struct {
	io.Closer
	mock.Mock
}

func (m *mockWebhookChanel) Trigger(attr scheduler.WebhookAttrs) {
	m.Called(attr)
}

func (m *mockWebhookChanel) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockAlertManager struct {
	io.Closer
	mock.Mock
}

func (m *mockAlertManager) Relay(attr *scheduler.AlertAttrs) {
	m.Called(attr)
}

func (m *mockAlertManager) Close() error {
	args := m.Called()
	return args.Error(0)
}
