package probebatch

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type leaseExpirer interface {
	ExpireDue(context.Context) (int, error)
}

type Service struct {
	store   *store.Store
	clock   platform.Clock
	ids     platform.IDGenerator
	expirer leaseExpirer
}

func New(
	stateStore *store.Store,
	clock platform.Clock,
	ids platform.IDGenerator,
	expirer leaseExpirer,
) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids, expirer: expirer}
}

// Accept validates every item (lease ownership, payload, intra-batch
// duplicates), orders items by lease id, and records an idempotent accepted
// job. Nothing is probed yet; Confirm or Resume performs the submissions.
func (s *Service) Accept(
	ctx context.Context,
	planID string,
	input domain.ProbeBatchInput,
) (domain.ProbeBatchJob, error) {
	now := s.clock.Now().UTC()
	if err := domain.ValidateProbeBatchInput(input, now); err != nil {
		return domain.ProbeBatchJob{}, err
	}
	input.RequestKey = strings.TrimSpace(input.RequestKey)

	// Stable order: sort a copy of the items by lease id. The original client
	// position is preserved so results can be correlated back to the request.
	type ordered struct {
		clientIndex int
		item        domain.ProbeBatchItemInput
	}
	orderedItems := make([]ordered, len(input.Items))
	for index, raw := range input.Items {
		orderedItems[index] = ordered{clientIndex: index, item: raw}
	}
	sort.SliceStable(orderedItems, func(i, j int) bool {
		return orderedItems[i].item.LeaseID < orderedItems[j].item.LeaseID
	})

	items := make([]domain.ProbeBatchItem, len(orderedItems))
	leaseCounts := make(map[string]int, len(orderedItems))
	for index, entry := range orderedItems {
		leaseID := strings.TrimSpace(entry.item.LeaseID)
		requestKey := strings.TrimSpace(entry.item.RequestKey)
		items[index] = domain.ProbeBatchItem{
			Index:          index,
			ClientIndex:    entry.clientIndex,
			LeaseID:        leaseID,
			RequestKey:     requestKey,
			Success:        entry.item.Success,
			ObservedSerial: entry.item.ObservedSerial,
			LatencyMS:      entry.item.LatencyMS,
			ObservedAt:     entry.item.ObservedAt.UTC(),
			Outcome:        domain.ProbeItemNotExecuted,
			PayloadFingerprint: domain.ProbeFingerprint(leaseID, domain.ProbeInput{
				RequestKey:     requestKey,
				Success:        entry.item.Success,
				ObservedSerial: entry.item.ObservedSerial,
				LatencyMS:      entry.item.LatencyMS,
				ObservedAt:     entry.item.ObservedAt,
			}),
		}
		leaseCounts[leaseID]++
	}

	fingerprint := domain.ProbeBatchFingerprint(planID, input, items)
	requestIndex := planID + "\x00" + input.RequestKey

	var job domain.ProbeBatchJob
	err := s.store.Write(ctx, func(state *domain.State) error {
		if _, ok := state.Plans[planID]; !ok {
			return domain.NotFound("plan", planID)
		}
		if existingID, exists := state.ProbeBatchRequestKeys[requestIndex]; exists {
			existing, found := state.ProbeBatches[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "probe batch index is inconsistent", map[string]any{
					"probe_batch_id": existingID,
				})
			}
			if existing.PayloadFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different batch content", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			job = existing
			return nil
		}

		details := make(map[string]any)
		for leaseID, count := range leaseCounts {
			lease, ok := state.Leases[leaseID]
			if !ok {
				details["lease_id:"+leaseID] = "lease was not found"
				continue
			}
			if lease.PlanID != planID {
				details["lease_id:"+leaseID] = "lease belongs to a different plan"
			}
			if count > 1 {
				details["lease_id:"+leaseID] = "lease appears more than once in the batch"
			}
		}
		if len(details) > 0 {
			return domain.Validation("probe batch items are invalid", details)
		}

		job = domain.ProbeBatchJob{
			ID:                 s.ids.New("pbj"),
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			PayloadFingerprint: fingerprint,
			State:              domain.ProbeBatchAccepted,
			ItemCount:          len(items),
			Checkpoint:         0,
			Items:              items,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		state.ProbeBatches[job.ID] = job
		state.ProbeBatchRequestKeys[requestIndex] = job.ID
		store.AddAudit(state, s.ids, "probe_batch", job.ID, "accepted", now, map[string]any{
			"plan_id":    planID,
			"item_count": len(items),
		})
		return nil
	})
	return job, err
}

// Confirm starts submitting an accepted (or interrupted) job from its
// explicit checkpoint. Re-confirming a completed job returns it unchanged.
func (s *Service) Confirm(ctx context.Context, batchID string) (domain.ProbeBatchJob, error) {
	return s.advance(ctx, batchID)
}

// Resume continues an interrupted (or accepted) job from its checkpoint. It
// shares the checkpointed kernel with Confirm, so it never re-executes an item
// that already reached a terminal outcome.
func (s *Service) Resume(ctx context.Context, batchID string) (domain.ProbeBatchJob, error) {
	return s.advance(ctx, batchID)
}

func (s *Service) Get(ctx context.Context, batchID string) (domain.ProbeBatchJob, error) {
	var job domain.ProbeBatchJob
	err := s.store.Read(ctx, func(state *domain.State) error {
		var ok bool
		job, ok = state.ProbeBatches[batchID]
		if !ok {
			return domain.NotFound("probe_batch", batchID)
		}
		return nil
	})
	return job, err
}

// advance drives one job forward, one item per transaction. Each committed
// transaction contains exactly one probe write (or one terminal rejection) and
// the matching item result plus checkpoint, so a crash or cancellation can
// never double-write a probe and a retry resumes at the first unexecuted item.
func (s *Service) advance(ctx context.Context, batchID string) (domain.ProbeBatchJob, error) {
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		if isContextError(err) {
			return s.markInterrupted(ctx, batchID)
		}
		return domain.ProbeBatchJob{}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return s.markInterrupted(ctx, batchID)
		}
		now := s.clock.Now().UTC()
		var done bool
		var job domain.ProbeBatchJob
		err := s.store.Write(ctx, func(state *domain.State) error {
			current, ok := state.ProbeBatches[batchID]
			if !ok {
				return domain.NotFound("probe_batch", batchID)
			}
			if current.State == domain.ProbeBatchCompleted {
				job = current
				done = true
				return nil
			}
			if current.Checkpoint >= len(current.Items) {
				current.State = domain.ProbeBatchCompleted
				current.UpdatedAt = now
				state.ProbeBatches[current.ID] = current
				job = current
				done = true
				return nil
			}
			if current.State != domain.ProbeBatchRunning {
				current.State = domain.ProbeBatchRunning
			}

			index := current.Checkpoint
			item := current.Items[index]
			lease, leaseFound := state.Leases[item.LeaseID]
			priorState := domain.LeaseState("")
			if leaseFound {
				priorState = lease.State
			}
			input := domain.ProbeInput{
				RequestKey:     item.RequestKey,
				Success:        item.Success,
				ObservedSerial: item.ObservedSerial,
				LatencyMS:      item.LatencyMS,
				ObservedAt:     item.ObservedAt,
			}
			result, applyErr := probe.ApplyOnState(state, s.ids, item.LeaseID, input, now)
			switch {
			case applyErr == nil:
				executedAt := now
				item.ExecutedAt = &executedAt
				item.ProbeID = result.ID
				item.ErrorCode = ""
				item.ErrorMessage = ""
				if priorState == domain.LeaseReported {
					item.Outcome = domain.ProbeItemReplay
				} else {
					item.Outcome = domain.ProbeItemSuccess
				}
			case isTransient(applyErr):
				// Item stays not_executed; the transaction is discarded so the
				// checkpoint does not move. A retry attempts the same item.
				return applyErr
			default:
				item = classifyRejected(item, leaseFound, lease, applyErr, now)
			}

			current.Items[index] = item
			current.Checkpoint = index + 1
			current.UpdatedAt = now
			if current.Checkpoint >= len(current.Items) {
				current.State = domain.ProbeBatchCompleted
				done = true
			}
			state.ProbeBatches[current.ID] = current
			job = current
			return nil
		})
		if err != nil {
			if isContextError(err) {
				return s.markInterrupted(ctx, batchID)
			}
			// A non-context failure leaves the checkpoint unchanged. Record the
			// interruption best-effort so the job advertises its resume point.
			_, _ = s.markInterrupted(context.WithoutCancel(ctx), batchID)
			return domain.ProbeBatchJob{}, err
		}
		if done {
			return job, nil
		}
	}
}

// classifyRejected turns a terminal kernel rejection into a per-item outcome.
// Leases in expired state (or rejected for being past expiry) map to expired;
// missing, released, replayed-with-different-content, and inactive-plan items
// map to conflict.
func classifyRejected(
	item domain.ProbeBatchItem,
	leaseFound bool,
	lease domain.NodeLease,
	err error,
	now time.Time,
) domain.ProbeBatchItem {
	executedAt := now
	item.ExecutedAt = &executedAt
	item.ProbeID = ""
	var domainErr *domain.DomainError
	if errors.As(err, &domainErr) {
		item.ErrorCode = string(domainErr.Code)
		item.ErrorMessage = domainErr.Message
	} else if err != nil {
		item.ErrorCode = string(domain.CodeInternal)
		item.ErrorMessage = err.Error()
	}
	switch {
	case item.ErrorCode == string(domain.CodeLeaseExpired):
		item.Outcome = domain.ProbeItemExpired
	case leaseFound && lease.State == domain.LeaseExpired:
		item.Outcome = domain.ProbeItemExpired
	default:
		item.Outcome = domain.ProbeItemConflict
	}
	return item
}

// isTransient reports whether a kernel error should leave the item unexecuted
// and interrupt the job for later retry.
func isTransient(err error) bool {
	if isContextError(err) {
		return true
	}
	var domainErr *domain.DomainError
	if errors.As(err, &domainErr) {
		return domainErr.Code == domain.CodeStorageFailure || domainErr.Code == domain.CodeInternal
	}
	return false
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// markInterrupted flips a non-completed job to "interrupted" with a detached
// context so the explicit checkpoint survives even when the caller's context
// has been cancelled. Items at or after the checkpoint stay not_executed.
func (s *Service) markInterrupted(originalCtx context.Context, batchID string) (domain.ProbeBatchJob, error) {
	ctx := context.WithoutCancel(originalCtx)
	now := s.clock.Now().UTC()
	var job domain.ProbeBatchJob
	err := s.store.Write(ctx, func(state *domain.State) error {
		current, ok := state.ProbeBatches[batchID]
		if !ok {
			return domain.NotFound("probe_batch", batchID)
		}
		if current.State != domain.ProbeBatchCompleted {
			current.State = domain.ProbeBatchInterrupted
			current.UpdatedAt = now
			state.ProbeBatches[current.ID] = current
		}
		job = current
		return nil
	})
	return job, err
}
