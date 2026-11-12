package utils

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
)

// RetryableHTTPStatusCodes contains the HTTP response status codes that may succeed on retry.
var RetryableHTTPStatusCodes = []int{429, 500, 502, 503, 504}

func IsRetryableHTTPStatus(statusCode int) bool {
	for _, code := range RetryableHTTPStatusCodes {
		if statusCode == code {
			return true
		}
	}
	return false
}

// IsRetryableTransportError reports transient failures from outbound HTTP calls.
func IsRetryableTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "eof") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "broken pipe")
}

// IsRetryableClientError reports whether an outbound HTTP client failure may succeed on retry.
// It walks the error chain and treats wrapped transport and retryable HTTP statuses as transient.
func IsRetryableClientError(err error) bool {
	if err == nil {
		return false
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if IsRetryableTransportError(e) {
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	for _, code := range RetryableHTTPStatusCodes {
		if strings.Contains(msg, "status code received "+strconv.Itoa(code)) {
			return true
		}
	}

	return IsRetryableTransportError(err)
}
