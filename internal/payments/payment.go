package payments

import "time"

type Payment struct {
	Amount        float64
	CorrelationId string
	RequestedAt   time.Time
	Processor     ProcessorType
}

func NewPayment(amountCents float64, correlationId string, processor ProcessorType, requestedAt time.Time) *Payment {
	return &Payment{
		Amount:        amountCents,
		CorrelationId: correlationId,
		RequestedAt:   requestedAt,
		Processor:     processor,
	}
}
