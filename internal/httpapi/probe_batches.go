package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) createProbeBatch(w http.ResponseWriter, r *http.Request) {
	var input domain.ProbeBatchInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	job, err := s.probeBatches.Accept(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, job)
}

func (s *Server) confirmProbeBatch(w http.ResponseWriter, r *http.Request) {
	// Confirm/Resume carry no required body; tolerate an empty or empty-object
	// body while still rejecting malformed JSON and unknown fields.
	if err := decodeOptionalEmpty(w, r); err != nil {
		sendError(w, r, err)
		return
	}
	job, err := s.probeBatches.Confirm(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, job)
}

func (s *Server) resumeProbeBatch(w http.ResponseWriter, r *http.Request) {
	if err := decodeOptionalEmpty(w, r); err != nil {
		sendError(w, r, err)
		return
	}
	job, err := s.probeBatches.Resume(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, job)
}

func (s *Server) getProbeBatch(w http.ResponseWriter, r *http.Request) {
	job, err := s.probeBatches.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, job)
}
