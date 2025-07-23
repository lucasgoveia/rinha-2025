package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"rinha/internal/payments"
	"time"
)

// ServiceHealth represents the health check response from a Payment service
type ServiceHealth struct {
	Failing         bool  `json:"failing"`
	MinResponseTime int64 `json:"minResponseTime"`
}

// ServiceMonitor monitors the health and performance of Payment services
type ServiceMonitor struct {
	httpClient    *http.Client
	checkInterval time.Duration
	logger        *slog.Logger
	ctx           context.Context
	cancel        context.CancelFunc

	defaultServiceURL  string
	fallbackServiceURL string

	servicesHealth map[payments.ServiceType]ServiceHealth
}

func NewServiceMonitor(defaultServiceURL, fallbackServiceURL string, httpClient *http.Client, logger *slog.Logger) *ServiceMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	monitor := &ServiceMonitor{
		httpClient:         httpClient,
		checkInterval:      5 * time.Second,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
		defaultServiceURL:  defaultServiceURL,
		fallbackServiceURL: fallbackServiceURL,
		servicesHealth:     make(map[payments.ServiceType]ServiceHealth),
	}

	return monitor
}

func (m *ServiceMonitor) StartMonitoring() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	m.checkServiceHealth(payments.ServiceTypeDefault)
	m.checkServiceHealth(payments.ServiceTypeFallback)

	for {
		select {
		case <-ticker.C:
			m.checkServiceHealth(payments.ServiceTypeDefault)
			m.checkServiceHealth(payments.ServiceTypeFallback)
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *ServiceMonitor) getServiceUrl(serviceType payments.ServiceType) string {
	if serviceType == payments.ServiceTypeDefault {
		return m.defaultServiceURL
	} else {
		return m.fallbackServiceURL
	}
}

func (m *ServiceMonitor) checkServiceHealth(serviceType payments.ServiceType) {
	healthURL := fmt.Sprintf("%s/service-health", m.getServiceUrl(serviceType))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		m.logger.Error("Failed to create health check request", "url", healthURL, "error", err)
		return
	}

	resp, err := m.httpClient.Do(req)

	if err != nil {
		m.logger.Error("Health check request failed", "url", healthURL, "error", err)
		m.updateServiceStatus(serviceType, true, 0)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Error("Health check returned non-OK status", "url", healthURL, "status", resp.StatusCode)
		m.updateServiceStatus(serviceType, true, 0)
		return
	}

	var health ServiceHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		m.logger.Error("Failed to decode health check response", "url", healthURL, "error", err)
		return
	}

	m.updateServiceStatus(serviceType, health.Failing, health.MinResponseTime)
}

func (m *ServiceMonitor) updateServiceStatus(service payments.ServiceType, failing bool, minResponseTime int64) {
	status := ServiceHealth{
		Failing:         failing,
		MinResponseTime: minResponseTime,
	}

	m.servicesHealth[service] = status
	m.logger.Debug("Service status updated", "service", service, "failing", failing, "minResponseTime", minResponseTime)
}

// Stop stops the monitoring
func (m *ServiceMonitor) Stop() {
	m.cancel()
}

type ServiceWorkFactor struct {
	DefaultWorkFactor  int
	FallbackWorkFactor int
}

const (
	feeWeight     = 100.0
	latencyWeight = 0.01
)

var (
	ErrBothServicesUnavailable = errors.New("both services unavailable")
)

func (m *ServiceMonitor) CalculateServiceRequests(n int) (ServiceWorkFactor, error) {
	defaultHealth := m.servicesHealth[payments.ServiceTypeDefault]
	fallbackHealth := m.servicesHealth[payments.ServiceTypeFallback]

	normalizeRT := func(rt int64) float64 {
		if rt <= 0 {
			return 1 // 1 ms
		}
		return float64(rt)
	}

	calcScore := func(h ServiceHealth, fee float64) float64 {
		if h.Failing {
			return 0
		}
		return 1 / ((fee * feeWeight) + (normalizeRT(h.MinResponseTime) * latencyWeight))
	}

	lMaxDefault := calcScore(defaultHealth, 0.05)
	lMaxFallback := calcScore(fallbackHealth, 0.15)

	switch {
	case lMaxDefault == 0 && lMaxFallback == 0:
		return ServiceWorkFactor{}, ErrBothServicesUnavailable
	case lMaxDefault == 0:
		return ServiceWorkFactor{DefaultWorkFactor: 0, FallbackWorkFactor: n}, nil
	case lMaxFallback == 0:
		return ServiceWorkFactor{DefaultWorkFactor: n, FallbackWorkFactor: 0}, nil
	}

	z := lMaxDefault + lMaxFallback

	cntDefault := int(math.Round((lMaxDefault / z) * float64(n)))
	cntFallback := n - cntDefault

	return ServiceWorkFactor{
		DefaultWorkFactor:  cntDefault,
		FallbackWorkFactor: cntFallback,
	}, nil
}

func (m *ServiceMonitor) CheckServiceAvailable(serviceType payments.ServiceType) bool {
	if val, ok := m.servicesHealth[serviceType]; ok {
		return !val.Failing
	}

	// presume is available
	return true
}

func (m *ServiceMonitor) InformFailure(serviceType payments.ServiceType) {
	var status ServiceHealth
	status.Failing = true
	status.MinResponseTime = 0

	if val, ok := m.servicesHealth[serviceType]; ok {
		status = val
	}

	status.Failing = true
	m.updateServiceStatus(serviceType, status.Failing, status.MinResponseTime)
}
