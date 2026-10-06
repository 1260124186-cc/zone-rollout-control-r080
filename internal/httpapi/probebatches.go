package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) submitProbeBatch(w http.ResponseWriter, r *http.Request) {
	var input domain.ProbeBatchInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	batch, created, err := s.probeBatches.Submit(r.Context(), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	sendData(w, r, status, batch)
}

func (s *Server) getProbeBatch(w http.ResponseWriter, r *http.Request) {
	batch, err := s.probeBatches.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, batch)
}
