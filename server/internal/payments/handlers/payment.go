package handlers

import (
	"io"
	"net/http"
	"rinha/internal/messages"
)

type PaymentHandler struct {
	publisher *messages.Publisher
}

func NewPaymentHandler(publisher *messages.Publisher) *PaymentHandler {
	return &PaymentHandler{publisher: publisher}
}

func (h *PaymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<8))
	if err != nil {
		_ = r.Body.Close()
		http.Error(w, "erro ao ler requisição", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	if err := h.publisher.Publish(raw); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
