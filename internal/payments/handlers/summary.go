package handlers

import (
	"github.com/bytedance/sonic"
	"net/http"
	"rinha/internal/payments"
	"time"
)

type SummaryHandler struct {
	store      *payments.PaymentStore
	httpClient *http.Client
}

func NewSummaryHandler(store *payments.PaymentStore, httpClient *http.Client) *SummaryHandler {
	return &SummaryHandler{
		store:      store,
		httpClient: httpClient,
	}
}

func (h *SummaryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var fromDate, toDate *time.Time

	fromParam := r.URL.Query().Get("from")
	if fromParam != "" {
		parsedTime, err := time.Parse(time.RFC3339, fromParam)
		if err == nil {
			fromDate = &parsedTime
		}
	}

	toParam := r.URL.Query().Get("to")
	if toParam != "" {
		parsedTime, err := time.Parse(time.RFC3339, toParam)
		if err == nil {
			toDate = &parsedTime
		}
	}

	summary, err := h.store.Summary(r.Context(), fromDate, toDate)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := sonic.ConfigFastest.NewEncoder(w).Encode(summary); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
