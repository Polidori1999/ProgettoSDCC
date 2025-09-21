package services

import (
	"ProgettoSDCC/pkg/proto"
	"errors"
)

var ErrUnknownService = errors.New("servizio sconosciuto")
var ErrDivisionByZero = errors.New("divisione per zero")

func Execute(service string, a, b float64) (float64, error) {
	switch service {
	case proto.ServiceSum:
		return a + b, nil
	case proto.ServiceSub:
		return a - b, nil
	case proto.ServiceMul:
		return a * b, nil
	case proto.ServiceDiv:
		if b == 0 {
			return 0, ErrDivisionByZero
		}
		return a / b, nil
	default:
		return 0, ErrUnknownService
	}
}
