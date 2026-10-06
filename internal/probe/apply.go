package probe

import (
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

// ApplyOnState is the single probe-write kernel shared by the single-probe
// entry and batch jobs. It checks the lease against the working state, returns
// the original probe on a same-key same-content replay, or writes exactly one
// probe, completes the lease, and releases the node. The mutation is visible
// only inside the surrounding store transaction and is discarded on error.
func ApplyOnState(
	state *domain.State,
	ids platform.IDGenerator,
	leaseID string,
	input domain.ProbeInput,
	now time.Time,
) (domain.Probe, error) {
	now = now.UTC()
	fingerprint := domain.ProbeFingerprint(leaseID, input)

	lease, ok := state.Leases[leaseID]
	if !ok {
		return domain.Probe{}, domain.NotFound("lease", leaseID)
	}
	if lease.State == domain.LeaseReported && lease.ProbeID != "" {
		existing, found := state.Probes[lease.ProbeID]
		if !found {
			return domain.Probe{}, domain.NewError(domain.CodeStorageFailure, "lease references a missing probe", map[string]any{
				"probe_id": lease.ProbeID,
			})
		}
		if existing.PayloadFingerprint != fingerprint {
			return domain.Probe{}, domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different probe content", map[string]any{
				"lease_id":    leaseID,
				"request_key": input.RequestKey,
			})
		}
		return existing, nil
	}
	if lease.State != domain.LeaseClaimed {
		return domain.Probe{}, domain.NewError(domain.CodeLeaseNotClaimed, "lease is not claimed", map[string]any{
			"lease_id": leaseID,
			"state":    lease.State,
		})
	}
	if !now.Before(lease.ExpiresAt) {
		return domain.Probe{}, domain.NewError(domain.CodeLeaseExpired, "lease has expired", map[string]any{
			"lease_id":   leaseID,
			"expires_at": lease.ExpiresAt,
		})
	}
	plan, ok := state.Plans[lease.PlanID]
	if !ok {
		return domain.Probe{}, domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
			"plan_id": lease.PlanID,
		})
	}
	if plan.State != domain.PlanActive {
		return domain.Probe{}, domain.InvalidState("plan must be active to store probes", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	node, ok := state.Nodes[lease.NodeID]
	if !ok {
		return domain.Probe{}, domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
			"node_id": lease.NodeID,
		})
	}
	result := domain.Probe{
		ID:                 ids.New("prb"),
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
	state.Probes[result.ID] = result
	lease.State = domain.LeaseReported
	finishedAt := now
	lease.FinishedAt = &finishedAt
	lease.ProbeID = result.ID
	state.Leases[leaseID] = lease
	if node.State == domain.NodeLeased {
		node.State = domain.NodeReady
		node.Version++
		node.UpdatedAt = now
		state.Nodes[node.ID] = node
	}
	store.AddAudit(state, ids, "probe", result.ID, "stored", now, map[string]any{
		"lease_id": leaseID,
		"plan_id":  lease.PlanID,
	})
	return result, nil
}
