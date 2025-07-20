package payments

import (
	"time"
)

type Payment struct {
	Amount        float64
	ServiceUsed   ServiceType
	RequestedAt   time.Time
	CorrelationId string
}
