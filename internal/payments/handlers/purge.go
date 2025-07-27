package handlers

import (
	"net/http"
	"rinha/internal/payments"
)

type PurgeHandler struct {
	store *payments.PaymentStore
}

func NewPurgeHandler(store *payments.PaymentStore) *PurgeHandler {
	return &PurgeHandler{store: store}
}

func (h *PurgeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.store.Purge(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
