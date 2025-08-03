package payments

import "time"

type PaymentMessage struct {
	Amount        float64   `json:"amount"`
	CorrelationId string    `json:"correlationId"`
	RequestedAt   time.Time `json:"requestedAt"`
	RetryCount    int       `json:"retryCount"`
}
