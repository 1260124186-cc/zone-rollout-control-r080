package probe

import (
	"context"
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

func (s *Service) Submit(
	ctx context.Context,
	leaseID string,
	input domain.ProbeInput,
) (domain.Probe, error) {
	now := s.clock.Now().UTC()
	if err := domain.ValidateProbeInput(input, now); err != nil {
		return domain.Probe{}, err
	}
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		return domain.Probe{}, err
	}
	fingerprint := domain.ProbeFingerprint(leaseID, input)
	var result domain.Probe
	err := s.store.Write(ctx, func(state *domain.State) error {
		existing, replay, err := domain.CheckProbeSubmission(state, leaseID, input.RequestKey, fingerprint, now)
		if err != nil {
			return err
		}
		if replay {
			result = existing
			return nil
		}
		lease := state.Leases[leaseID]
		result = domain.Probe{
			ID:                 s.ids.New("prb"),
			LeaseID:            leaseID,
			PlanID:             lease.PlanID,
			NodeID:             lease.NodeID,
			RequestKey:         input.RequestKey,
			Success:            input.Success,
			ObservedSerial:     input.ObservedSerial,
			LatencyMS:          input.LatencyMS,
			ObservedAt:         input.ObservedAt.UTC(),
			StoredAt:           now,
			PayloadFingerprint: fingerprint,
		}
		domain.ApplyProbe(state, result, now)
		store.AddAudit(state, s.ids, "probe", result.ID, "stored", now, map[string]any{
			"lease_id": leaseID,
			"plan_id":  lease.PlanID,
		})
		return nil
	})
	return result, err
}

func pointer(value time.Time) *time.Time {
	return &value
}
