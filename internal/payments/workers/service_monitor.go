package workers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"rinha/internal/payments"
	"time"
)

type ProcessorHealth struct {
	Failing         bool  `json:"failing"`
	MinResponseTime int64 `json:"minResponseTime"`
}

type ProcessorHealthMonitor struct {
	logger                   *slog.Logger
	defaultServiceHealthURL  string
	fallbackServiceHealthURL string
	httpClient               *http.Client
	done                     chan struct{}
	processorsHealths        map[payments.ProcessorType]ProcessorHealth
}

func NewServiceMonitor(defaultServiceURL, fallbackServiceURL string, httpClient *http.Client, logger *slog.Logger) *ProcessorHealthMonitor {

	monitor := &ProcessorHealthMonitor{
		httpClient:               httpClient,
		logger:                   logger,
		defaultServiceHealthURL:  defaultServiceURL,
		fallbackServiceHealthURL: fallbackServiceURL,
		processorsHealths:        make(map[payments.ProcessorType]ProcessorHealth),
	}

	return monitor
}

func (m *ProcessorHealthMonitor) StartMonitoring() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	m.checkProcessorHealth(payments.ProcessorTypeDefault)
	m.checkProcessorHealth(payments.ProcessorTypeFallback)

	for {
		select {
		case <-ticker.C:
			m.checkProcessorHealth(payments.ProcessorTypeDefault)
			m.checkProcessorHealth(payments.ProcessorTypeFallback)
		case <-m.done:
			return
		}
	}
}

func (m *ProcessorHealthMonitor) getServiceUrl(processorType payments.ProcessorType) string {
	if processorType == payments.ProcessorTypeDefault {
		return m.defaultServiceHealthURL
	} else {
		return m.fallbackServiceHealthURL
	}
}

func (m *ProcessorHealthMonitor) checkProcessorHealth(processorType payments.ProcessorType) {
	healthURL := m.getServiceUrl(processorType)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		m.logger.Error("Failed to create health check request", "url", healthURL, "error", err)
		return
	}

	resp, err := m.httpClient.Do(req)

	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		m.logger.Error("Health check request failed", "url", healthURL, "error", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		m.logger.Error("Health check returned non-OK status", "url", healthURL, "status", resp.StatusCode)
		return
	}

	var health ProcessorHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		m.logger.Error("Failed to decode health check response", "url", healthURL, "error", err)
		return
	}

	m.updateServiceStatus(processorType, health.Failing, health.MinResponseTime)
}

func (m *ProcessorHealthMonitor) updateServiceStatus(processor payments.ProcessorType, failing bool, minResponseTime int64) {
	status := ProcessorHealth{
		Failing:         failing,
		MinResponseTime: minResponseTime,
	}

	m.processorsHealths[processor] = status
	//m.logger.Debug("Service status updated", "processor", processor, "failing", failing, "minResponseTime", minResponseTime)
}

// Stop stops the monitoring
func (m *ProcessorHealthMonitor) Stop() {
	m.done <- struct{}{}
	close(m.done)
}

var (
	ErrBothProcessorsUnavailable = errors.New("both services unavailable")
)

const (
	maxAcceptableMinResponseTime = 120
)

func (m *ProcessorHealthMonitor) DetermineProcessor() (payments.ProcessorType, error) {
	defaultHealth := m.processorsHealths[payments.ProcessorTypeDefault]
	fallbackHealth := m.processorsHealths[payments.ProcessorTypeFallback]

	defaultFailing := defaultHealth.Failing || defaultHealth.MinResponseTime > maxAcceptableMinResponseTime
	fallbackFailing := fallbackHealth.Failing || fallbackHealth.MinResponseTime > maxAcceptableMinResponseTime

	if defaultFailing && fallbackFailing {
		return "", ErrBothProcessorsUnavailable
	}

	if defaultFailing {
		return payments.ProcessorTypeFallback, nil
	}

	if fallbackFailing {
		return payments.ProcessorTypeDefault, nil
	}

	if defaultHealth.MinResponseTime <= (3 * fallbackHealth.MinResponseTime) {
		return payments.ProcessorTypeDefault, nil
	}

	return payments.ProcessorTypeFallback, nil
}

func (m *ProcessorHealthMonitor) InformFailure(processorType payments.ProcessorType) {
	var status ProcessorHealth
	status.Failing = true
	status.MinResponseTime = 0

	if val, ok := m.processorsHealths[processorType]; ok {
		status = val
	}

	status.Failing = true
	m.updateServiceStatus(processorType, status.Failing, status.MinResponseTime)
}
