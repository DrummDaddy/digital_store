package httpapi

import (
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) catalog(
	w http.ResponseWriter,
	r *http.Request,
) {
	if s.catalogRepo == nil {
		writeError(
			w,
			http.StatusServiceUnavailable,
			"catalog service is not configured",
		)
		return
	}

	limit := 50

	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		value, err := strconv.Atoi(rawLimit)
		if err != nil {
			writeError(
				w,
				http.StatusBadRequest,
				"invalid limit",
			)
			return
		}

		limit = value
	}

	if limit < 1 || limit > 100 {
		writeError(
			w,
			http.StatusBadRequest,
			"limit must be between 1 and 100",
		)
		return
	}

	afterSKU := strings.TrimSpace(
		r.URL.Query().Get("after"),
	)

	page, err := s.catalogRepo.List(
		r.Context(),
		limit,
		afterSKU,
	)
	if err != nil {
		s.logger.Error(
			"catalog request failed",
			"error", err,
			"limit", limit,
			"after", afterSKU,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"internal server error",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		page,
	)
}
