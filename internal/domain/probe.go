package domain

import "time"

// CheckProbeSubmission inspects the current state for a probe submission
// against leaseID. It returns the stored probe and true when the submission
// is an exact replay of an already recorded probe. Every rejection is a
// domain error carrying one of the stable probe error codes, so both the
// single-probe endpoint and probe batch jobs reach identical decisions for
// the same lease state.
func CheckProbeSubmission(
	state *State,
	leaseID string,
	requestKey string,
	fingerprint string,
	now time.Time,
) (Probe, bool, error) {
	lease, ok := state.Leases[leaseID]
	if !ok {
		return Probe{}, false, NotFound("lease", leaseID)
	}
	if lease.State == LeaseReported && lease.ProbeID != "" {
		existing, found := state.Probes[lease.ProbeID]
		if !found {
			return Probe{}, false, NewError(CodeStorageFailure, "lease references a missing probe", map[string]any{
				"probe_id": lease.ProbeID,
			})
		}
		if existing.PayloadFingerprint != fingerprint {
			return Probe{}, false, NewError(CodeIdempotencyConflict, "request key was reused with different probe content", map[string]any{
				"lease_id":    leaseID,
				"request_key": requestKey,
			})
		}
		return existing, true, nil
	}
	if lease.State != LeaseClaimed {
		return Probe{}, false, NewError(CodeLeaseNotClaimed, "lease is not claimed", map[string]any{
			"lease_id": leaseID,
			"state":    lease.State,
		})
	}
	if !now.Before(lease.ExpiresAt) {
		return Probe{}, false, NewError(CodeLeaseExpired, "lease has expired", map[string]any{
			"lease_id":   leaseID,
			"expires_at": lease.ExpiresAt,
		})
	}
	plan, ok := state.Plans[lease.PlanID]
	if !ok {
		return Probe{}, false, NewError(CodeStorageFailure, "lease references a missing plan", map[string]any{
			"plan_id": lease.PlanID,
		})
	}
	if plan.State != PlanActive {
		return Probe{}, false, InvalidState("plan must be active to store probes", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if _, ok := state.Nodes[lease.NodeID]; !ok {
		return Probe{}, false, NewError(CodeStorageFailure, "lease references a missing node", map[string]any{
			"node_id": lease.NodeID,
		})
	}
	return Probe{}, false, nil
}

// ApplyProbe records a decided probe, completes its lease, and returns the
// node to the ready pool. It must run inside a store transaction together
// with the caller's own bookkeeping.
func ApplyProbe(state *State, probe Probe, now time.Time) {
	state.Probes[probe.ID] = probe
	lease := state.Leases[probe.LeaseID]
	lease.State = LeaseReported
	finishedAt := now.UTC()
	lease.FinishedAt = &finishedAt
	lease.ProbeID = probe.ID
	state.Leases[lease.ID] = lease
	if node, ok := state.Nodes[lease.NodeID]; ok && node.State == NodeLeased {
		node.State = NodeReady
		node.Version++
		node.UpdatedAt = now.UTC()
		state.Nodes[node.ID] = node
	}
}
