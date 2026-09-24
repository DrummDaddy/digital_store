package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type HTTPProvider struct {
	name       string
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
}

func NewHTTPProvider(
	name string,
	baseURL string,
	timeout time.Duration,
) *HTTPProvider {
	return &HTTPProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		timeout: timeout,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (p *HTTPProvider) Name() string {
	return p.name
}

func (p *HTTPProvider) Issue(
	ctx context.Context,
	request IssueRequest,
) (IssueResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return IssueResponse{}, fmt.Errorf(
			"marshal provider request: %w",
			err,
		)
	}

	requestCtx, cancel := context.WithTimeout(
		ctx,
		p.timeout,
	)
	defer cancel()

	httpRequest, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		p.baseURL+"/issue",
		bytes.NewReader(payload),
	)
	if err != nil {
		return IssueResponse{}, fmt.Errorf(
			"create provider request: %w",
			err,
		)
	}

	httpRequest.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err := p.httpClient.Do(httpRequest)
	if err != nil {
		if isTimeoutError(err) ||
			errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return IssueResponse{}, fmt.Errorf(
				"%w: provider=%s",
				ErrProviderTimeout,
				p.name,
			)
		}

		return IssueResponse{}, fmt.Errorf(
			"%w: provider=%s: %v",
			ErrProviderUnavailable,
			p.name,
			err,
		)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(
		io.LimitReader(response.Body, 1<<20),
	)
	if err != nil {
		return IssueResponse{}, fmt.Errorf(
			"read provider response: %w",
			err,
		)
	}

	var result IssueResponse

	if len(body) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			return IssueResponse{}, fmt.Errorf(
				"%w: provider=%s: %v",
				ErrInvalidResponse,
				p.name,
				err,
			)
		}
	}

	switch {
	case response.StatusCode == http.StatusOK:
		if result.Status != "ok" ||
			result.RequestID != request.RequestID ||
			result.Code == "" {
			return IssueResponse{}, fmt.Errorf(
				"%w: provider=%s",
				ErrInvalidResponse,
				p.name,
			)
		}

		return result, nil

	case response.StatusCode == http.StatusConflict:
		if result.Reason == "out_of_stock" {
			return IssueResponse{}, fmt.Errorf(
				"%w: provider=%s",
				ErrOutOfStock,
				p.name,
			)
		}

		return IssueResponse{}, fmt.Errorf(
			"%w: provider=%s, status=%d",
			ErrInvalidResponse,
			p.name,
			response.StatusCode,
		)

	case response.StatusCode >= 500:
		return IssueResponse{}, fmt.Errorf(
			"%w: provider=%s, status=%d",
			ErrProviderUnavailable,
			p.name,
			response.StatusCode,
		)

	default:
		return IssueResponse{}, fmt.Errorf(
			"%w: provider=%s, status=%d",
			ErrInvalidResponse,
			p.name,
			response.StatusCode,
		)
	}
}

func isTimeoutError(err error) bool {
	var networkError net.Error

	return errors.As(err, &networkError) &&
		networkError.Timeout()
}
