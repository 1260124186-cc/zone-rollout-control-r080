// Package probebatch accepts a batch of probe submissions as one idempotent
// job. Intake validates every item (payload, lease, in-batch duplicates) and
// records the verdicts on a persistent ledger; the commit phase then decides
// the remaining items in stable order, one store transaction per item, so an
// interrupted job resumes from the persisted checkpoint instead of starting
// over or writing probes twice.
package probebatch

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type LeaseExpirer interface {
	ExpireDue(context.Context) (int, error)
}

type Service struct {
	store   *store.Store
	clock   platform.Clock
	ids     platform.IDGenerator
	expirer LeaseExpirer
}

func New(
	stateStore *store.Store,
	clock platform.Clock,
	ids platform.IDGenerator,
	expirer LeaseExpirer,
) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids, expirer: expirer}
}

// Submit accepts a new batch or resumes the batch previously accepted under
// the same request key. It returns the batch ledger once every item has a
// final outcome. The boolean reports whether this call created the batch.
func (s *Service) Submit(
	ctx context.Context,
	input domain.ProbeBatchInput,
) (domain.ProbeBatch, bool, error) {
	if err := domain.ValidateProbeBatchInput(input); err != nil {
		return domain.ProbeBatch{}, false, err
	}
	input.RequestKey = strings.TrimSpace(input.RequestKey)
	fingerprint := domain.ProbeBatchFingerprint(input)
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		return domain.ProbeBatch{}, false, err
	}
	now := s.clock.Now().UTC()
	var batch domain.ProbeBatch
	created := false
	err := s.store.Write(ctx, func(state *domain.State) error {
		if existingID, exists := state.ProbeBatchRequestKeys[input.RequestKey]; exists {
			existing, found := state.ProbeBatches[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "probe batch request index is inconsistent", map[string]any{
					"probe_batch_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different batch content", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			batch = existing
			return nil
		}
		batch = s.intake(state, input, fingerprint, now)
		state.ProbeBatches[batch.ID] = batch
		state.ProbeBatchRequestKeys[input.RequestKey] = batch.ID
		store.AddAudit(state, s.ids, "probe_batch", batch.ID, "accepted", now, map[string]any{
			"request_key": batch.RequestKey,
			"total":       batch.Total,
		})
		created = true
		return nil
	})
	if err != nil {
		return domain.ProbeBatch{}, false, err
	}
	batch, err = s.commit(ctx, batch.ID, now)
	if err != nil {
		return batch, created, err
	}
	return batch, created, nil
}

// Get returns the current ledger of one batch job.
func (s *Service) Get(ctx context.Context, batchID string) (domain.ProbeBatch, error) {
	var batch domain.ProbeBatch
	err := s.store.Read(ctx, func(state *domain.State) error {
		found, ok := state.ProbeBatches[batchID]
		if !ok {
			return domain.NotFound("probe_batch", batchID)
		}
		batch = found
		return nil
	})
	return batch, err
}

// intake validates every submitted item against the current state and builds
// the batch ledger in stable commit order. Items that fail intake keep a
// final not_executed (or conflict) outcome with the rejecting error; the
// remaining items stay pending for the commit phase.
func (s *Service) intake(
	state *domain.State,
	input domain.ProbeBatchInput,
	fingerprint string,
	now time.Time,
) domain.ProbeBatch {
	items := make([]domain.ProbeBatchItem, 0, len(input.Items))
	for position, submitted := range input.Items {
		items = append(items, domain.ProbeBatchItem{
			Position:       position,
			LeaseID:        strings.TrimSpace(submitted.LeaseID),
			RequestKey:     strings.TrimSpace(submitted.RequestKey),
			Success:        submitted.Success,
			ObservedSerial: submitted.ObservedSerial,
			LatencyMS:      submitted.LatencyMS,
			ObservedAt:     submitted.ObservedAt.UTC(),
			Outcome:        domain.ProbeBatchItemNotExecuted,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].LeaseID != items[j].LeaseID {
			return items[i].LeaseID < items[j].LeaseID
		}
		if items[i].RequestKey != items[j].RequestKey {
			return items[i].RequestKey < items[j].RequestKey
		}
		return items[i].Position < items[j].Position
	})
	executable := make(map[string]bool)
	for index := range items {
		item := &items[index]
		if item.LeaseID == "" {
			item.Error = itemError(domain.CodeValidationFailed, "lease_id is required", nil)
			continue
		}
		payload := domain.ProbeInput{
			RequestKey:     item.RequestKey,
			Success:        item.Success,
			ObservedSerial: item.ObservedSerial,
			LatencyMS:      item.LatencyMS,
			ObservedAt:     item.ObservedAt,
		}
		if err := domain.ValidateProbeInput(payload, now); err != nil {
			item.Error = errorFrom(err)
			continue
		}
		if _, ok := state.Leases[item.LeaseID]; !ok {
			item.Error = itemError(domain.CodeNotFound, "lease was not found", map[string]any{
				"lease_id": item.LeaseID,
			})
			continue
		}
		if executable[item.LeaseID] {
			item.Outcome = domain.ProbeBatchItemConflict
			item.Error = itemError(domain.CodeConflict, "lease appears more than once in the batch", map[string]any{
				"lease_id": item.LeaseID,
			})
			continue
		}
		executable[item.LeaseID] = true
	}
	batch := domain.ProbeBatch{
		ID:                 s.ids.New("pbt"),
		RequestKey:         input.RequestKey,
		RequestFingerprint: fingerprint,
		State:              domain.ProbeBatchAccepted,
		Total:              len(items),
		Items:              items,
		CreatedAt:          now,
	}
	batch.CommittedCount = committedCount(batch.Items)
	return batch
}

// commit decides pending items in stable order, one transaction per item, so
// the persisted ledger always reflects exactly which items are done. A
// transient failure aborts the loop; the next Submit with the same request
// key resumes at the first pending item.
func (s *Service) commit(ctx context.Context, batchID string, now time.Time) (domain.ProbeBatch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return domain.ProbeBatch{}, err
		}
		done := false
		var batch domain.ProbeBatch
		err := s.store.Write(ctx, func(state *domain.State) error {
			current, ok := state.ProbeBatches[batchID]
			if !ok {
				return domain.NewError(domain.CodeStorageFailure, "probe batch disappeared during commit", map[string]any{
					"probe_batch_id": batchID,
				})
			}
			index := nextPending(current.Items)
			if index < 0 {
				if current.State != domain.ProbeBatchCommitted {
					current.State = domain.ProbeBatchCommitted
					finishedAt := now
					current.FinishedAt = &finishedAt
					state.ProbeBatches[batchID] = current
					store.AddAudit(state, s.ids, "probe_batch", batchID, "committed", now, map[string]any{
						"total":           current.Total,
						"committed_count": current.CommittedCount,
					})
				}
				batch = current
				done = true
				return nil
			}
			item := current.Items[index]
			outcome, probe, itemErr, err := s.decide(state, item, now)
			if err != nil {
				return err
			}
			current.Items[index].Outcome = outcome
			current.Items[index].ProbeID = probe
			current.Items[index].Error = itemErr
			current.CommittedCount = committedCount(current.Items)
			state.ProbeBatches[batchID] = current
			if outcome == domain.ProbeBatchItemStored {
				store.AddAudit(state, s.ids, "probe", probe, "stored", now, map[string]any{
					"lease_id":       item.LeaseID,
					"probe_batch_id": batchID,
				})
			}
			batch = current
			return nil
		})
		if err != nil {
			return batch, err
		}
		if done {
			return batch, nil
		}
	}
}

// decide evaluates one pending item against the current state using the same
// rules as the single-probe endpoint and applies the probe mutation when the
// item stores. Item-level rejections become ledger outcomes; only storage
// inconsistencies abort the job.
func (s *Service) decide(
	state *domain.State,
	item domain.ProbeBatchItem,
	now time.Time,
) (domain.ProbeBatchItemOutcome, string, *domain.ProbeBatchItemError, error) {
	lease, ok := state.Leases[item.LeaseID]
	if !ok {
		return "", "", nil, domain.NewError(domain.CodeStorageFailure, "batch item references a missing lease", map[string]any{
			"lease_id": item.LeaseID,
		})
	}
	if lease.State == domain.LeaseExpired {
		return domain.ProbeBatchItemExpired, "", itemError(domain.CodeLeaseExpired, "lease has expired", map[string]any{
			"lease_id":   item.LeaseID,
			"expires_at": lease.ExpiresAt,
		}), nil
	}
	payload := domain.ProbeInput{
		RequestKey:     item.RequestKey,
		Success:        item.Success,
		ObservedSerial: item.ObservedSerial,
		LatencyMS:      item.LatencyMS,
		ObservedAt:     item.ObservedAt,
	}
	fingerprint := domain.ProbeFingerprint(item.LeaseID, payload)
	existing, replay, err := domain.CheckProbeSubmission(state, item.LeaseID, item.RequestKey, fingerprint, now)
	if err != nil {
		switch domain.ErrorCodeOf(err) {
		case domain.CodeIdempotencyConflict, domain.CodeLeaseNotClaimed, domain.CodeInvalidState:
			return domain.ProbeBatchItemConflict, "", errorFrom(err), nil
		case domain.CodeLeaseExpired:
			return domain.ProbeBatchItemExpired, "", errorFrom(err), nil
		default:
			return "", "", nil, err
		}
	}
	if replay {
		return domain.ProbeBatchItemReplayed, existing.ID, nil, nil
	}
	probe := domain.Probe{
		ID:                 s.ids.New("prb"),
		LeaseID:            item.LeaseID,
		PlanID:             lease.PlanID,
		NodeID:             lease.NodeID,
		RequestKey:         item.RequestKey,
		Success:            item.Success,
		ObservedSerial:     item.ObservedSerial,
		LatencyMS:          item.LatencyMS,
		ObservedAt:         item.ObservedAt.UTC(),
		StoredAt:           now,
		PayloadFingerprint: fingerprint,
	}
	domain.ApplyProbe(state, probe, now)
	return domain.ProbeBatchItemStored, probe.ID, nil, nil
}

// nextPending returns the index of the first item that still waits for a
// commit decision. Intake rejections carry an error and are already final.
func nextPending(items []domain.ProbeBatchItem) int {
	for index := range items {
		if items[index].Outcome == domain.ProbeBatchItemNotExecuted && items[index].Error == nil {
			return index
		}
	}
	return -1
}

// committedCount is the checkpoint: how many items hold a final outcome.
func committedCount(items []domain.ProbeBatchItem) int {
	count := 0
	for _, item := range items {
		if item.Outcome != domain.ProbeBatchItemNotExecuted || item.Error != nil {
			count++
		}
	}
	return count
}

func itemError(code domain.ErrorCode, message string, details map[string]any) *domain.ProbeBatchItemError {
	return &domain.ProbeBatchItemError{
		Code:    string(code),
		Message: message,
		Details: details,
	}
}

func errorFrom(err error) *domain.ProbeBatchItemError {
	var target *domain.DomainError
	if !errors.As(err, &target) {
		return itemError(domain.CodeInternal, "internal error", nil)
	}
	return itemError(target.Code, target.Message, target.Details)
}
