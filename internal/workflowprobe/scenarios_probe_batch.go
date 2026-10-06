package workflowprobe

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func probeBatchItem(h *harness, leaseID string, requestKey string, serial uint32, latency int) domain.ProbeBatchItemInput {
	return domain.ProbeBatchItemInput{
		LeaseID: leaseID,
		ProbeInput: domain.ProbeInput{
			RequestKey:     requestKey,
			Success:        true,
			ObservedSerial: serial,
			LatencyMS:      latency,
			ObservedAt:     h.clock.Now(),
		},
	}
}

func createProbeBatch(h *harness, planID string, key string, items []domain.ProbeBatchItemInput) (domain.ProbeBatchJob, error) {
	var job domain.ProbeBatchJob
	err := h.call(http.MethodPut, "/v1/plans/"+planID+"/probe-batches", domain.ProbeBatchInput{
		RequestKey: key,
		Items:      items,
	}, &job)
	return job, err
}

func confirmProbeBatch(h *harness, batchID string) (domain.ProbeBatchJob, error) {
	var job domain.ProbeBatchJob
	err := h.call(http.MethodPatch, "/v1/probe-batches/"+batchID+"/confirm", nil, &job)
	return job, err
}

func resumeProbeBatch(h *harness, batchID string) (domain.ProbeBatchJob, error) {
	var job domain.ProbeBatchJob
	err := h.call(http.MethodPatch, "/v1/probe-batches/"+batchID+"/resume", nil, &job)
	return job, err
}

func getProbeBatch(h *harness, batchID string) (domain.ProbeBatchJob, error) {
	var job domain.ProbeBatchJob
	err := h.call(http.MethodGet, "/v1/probe-batches/"+batchID, nil, &job)
	return job, err
}

func outcomeIndex(job domain.ProbeBatchJob) map[string]domain.ProbeBatchItem {
	byLease := make(map[string]domain.ProbeBatchItem, len(job.Items))
	for _, item := range job.Items {
		byLease[item.LeaseID] = item
	}
	return byLease
}

func runProbeBatchJob() error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	for index := 1; index <= 6; index++ {
		name := fmt.Sprintf("resolver-batch-%02d", index)
		if _, err := enrollNode(h, name, "central"); err != nil {
			return err
		}
	}
	plan, revision, err := createActivePlan(h, "batch.example", 2026100601, 6, 1, 50)
	if err != nil {
		return err
	}

	// Four distinct leases for the main batch. Lease ids are random, so derive
	// the stable (sorted) order that acceptance is expected to produce.
	group, err := allocate(h, plan.ID, "batch-lease-0001", "central", 4)
	if err != nil {
		return err
	}
	sortedLeases := append([]string(nil), group.LeaseIDs...)
	sort.Strings(sortedLeases)
	leaseA, leaseB, leaseC, leaseD := sortedLeases[0], sortedLeases[1], sortedLeases[2], sortedLeases[3]

	// Submit items out of order to prove stable ordering by lease id.
	items := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseD, "batch-prb-0004", revision.Serial, 14),
		probeBatchItem(h, leaseB, "batch-prb-0002", revision.Serial, 11),
		probeBatchItem(h, leaseA, "batch-prb-0001", revision.Serial, 10),
		probeBatchItem(h, leaseC, "batch-prb-0003", revision.Serial, 12),
	}
	job, err := createProbeBatch(h, plan.ID, "batch-job-0001", items)
	if err != nil {
		return err
	}
	if job.State != domain.ProbeBatchAccepted || job.Checkpoint != 0 || job.ItemCount != 4 {
		return ensure(false, "new batch job was not accepted with checkpoint 0")
	}
	if job.Items[0].LeaseID != leaseA || job.Items[len(job.Items)-1].LeaseID != leaseD {
		return ensure(false, "batch items were not ordered by lease id")
	}
	if job.Items[0].ClientIndex != 2 || job.Items[3].ClientIndex != 0 {
		return ensure(false, "batch items lost their original positions")
	}
	fetched, err := getProbeBatch(h, job.ID)
	if err != nil {
		return err
	}
	for _, item := range fetched.Items {
		if item.Outcome != domain.ProbeItemNotExecuted {
			return ensure(false, "accepted item was executed before confirmation")
		}
	}

	// Re-accept with the same key and content returns the same job (no write).
	replay, err := createProbeBatch(h, plan.ID, "batch-job-0001", items)
	if err != nil {
		return err
	}
	if replay.ID != job.ID {
		return ensure(false, "batch replay created a new job")
	}

	// Cancellation before any item must leave an explicit checkpoint at zero;
	// resume then continues without double-writing anything.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = h.callCtx(canceledCtx, http.MethodPatch, "/v1/probe-batches/"+job.ID+"/confirm", nil, nil)
	if err == nil {
		return ensure(false, "cancelled confirm unexpectedly succeeded")
	}
	interrupted, err := getProbeBatch(h, job.ID)
	if err != nil {
		return err
	}
	if interrupted.Checkpoint != 0 {
		return ensure(false, "cancelled confirm advanced the checkpoint")
	}
	if interrupted.State != domain.ProbeBatchAccepted && interrupted.State != domain.ProbeBatchInterrupted {
		return ensure(false, "unexecuted batch did not remain resumable")
	}
	for _, item := range interrupted.Items {
		if item.Outcome != domain.ProbeItemNotExecuted {
			return ensure(false, "cancelled confirm executed an item")
		}
	}

	// Resume drives the whole batch to completion.
	completed, err := resumeProbeBatch(h, job.ID)
	if err != nil {
		return err
	}
	if completed.State != domain.ProbeBatchCompleted || completed.Checkpoint != 4 {
		return ensure(false, "resume did not complete the batch")
	}
	results := outcomeIndex(completed)
	for _, leaseID := range []string{leaseA, leaseB, leaseC, leaseD} {
		item := results[leaseID]
		if item.Outcome != domain.ProbeItemSuccess || item.ProbeID == "" {
			return ensure(false, "item did not succeed: "+leaseID)
		}
	}
	// The probe request key and original lease close the loop with the stored
	// probe: replaying through the ordinary single-probe entry returns the
	// exact probe the batch stored.
	for _, probeItem := range []struct {
		lease   string
		key     string
		latency int
	}{
		{leaseA, "batch-prb-0001", 10},
		{leaseB, "batch-prb-0002", 11},
		{leaseC, "batch-prb-0003", 12},
		{leaseD, "batch-prb-0004", 14},
	} {
		var stored domain.Probe
		path := "/v1/leases/" + probeItem.lease + "/probes"
		err := h.call(http.MethodPatch, path, domain.ProbeInput{
			RequestKey:     probeItem.key,
			Success:        true,
			ObservedSerial: revision.Serial,
			LatencyMS:      probeItem.latency,
			ObservedAt:     h.clock.Now(),
		}, &stored)
		if err != nil {
			return err
		}
		if stored.ID != results[probeItem.lease].ProbeID {
			return ensure(false, "single-entry replay did not close to the batch probe")
		}
	}

	// Confirm/resume on a completed job are idempotent no-ops.
	again, err := confirmProbeBatch(h, job.ID)
	if err != nil || again.State != domain.ProbeBatchCompleted || again.Checkpoint != 4 {
		return ensure(false, "completed batch confirm was not idempotent")
	}

	// Same batch key with different content is rejected.
	conflictItems := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseA, "batch-prb-0001", revision.Serial, 99),
	}
	_, err = createProbeBatch(h, plan.ID, "batch-job-0001", conflictItems)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// Second batch over the same reported leases: identical content replays
	// each probe; changed content conflicts per item.
	replayItems := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseA, "batch-prb-0001", revision.Serial, 10),
		probeBatchItem(h, leaseB, "batch-prb-0002", revision.Serial, 40),
	}
	replayJob, err := createProbeBatch(h, plan.ID, "batch-job-0002", replayItems)
	if err != nil {
		return err
	}
	replayJob, err = confirmProbeBatch(h, replayJob.ID)
	if err != nil {
		return err
	}
	replayResults := outcomeIndex(replayJob)
	if replayResults[leaseA].Outcome != domain.ProbeItemReplay ||
		replayResults[leaseA].ProbeID != results[leaseA].ProbeID {
		return ensure(false, "identical item was not replayed to the original probe")
	}
	if replayResults[leaseB].Outcome != domain.ProbeItemConflict ||
		replayResults[leaseB].ProbeID != "" {
		return ensure(false, "changed item on a reported lease was not a conflict")
	}

	// Fifth and sixth nodes: an expired lease maps to expired, a released
	// lease to conflict, and later items still execute.
	extra, err := allocate(h, plan.ID, "batch-lease-0002", "central", 2)
	if err != nil {
		return err
	}
	leaseE, leaseF := extra.LeaseIDs[0], extra.LeaseIDs[1]
	if err := h.call(http.MethodPatch, "/v1/leases/"+leaseF+"/release", nil, nil); err != nil {
		return err
	}
	h.clock.Advance(601_000_000_000) // lease window was 600 seconds
	terminalItems := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseE, "batch-prb-0005", revision.Serial, 10),
		probeBatchItem(h, leaseF, "batch-prb-0006", revision.Serial, 10),
	}
	terminalJob, err := createProbeBatch(h, plan.ID, "batch-job-0003", terminalItems)
	if err != nil {
		return err
	}
	terminalJob, err = confirmProbeBatch(h, terminalJob.ID)
	if err != nil {
		return err
	}
	if terminalJob.State != domain.ProbeBatchCompleted || terminalJob.Checkpoint != 2 {
		return ensure(false, "terminal-status batch did not complete")
	}
	terminalResults := outcomeIndex(terminalJob)
	if terminalResults[leaseE].Outcome != domain.ProbeItemExpired {
		return ensure(false, "expired lease was not reported as expired")
	}
	if terminalResults[leaseF].Outcome != domain.ProbeItemConflict {
		return ensure(false, "released lease was not reported as conflict")
	}

	// Mid-batch interruption: cancel confirm partway through and resume until
	// completed. The exact stopping point is timing dependent; the invariant is
	// that the checkpoint never moves past executed items and resume never
	// double-writes a probe. Uses a dedicated 10-node plan.
	for index := 7; index <= 10; index++ {
		name := fmt.Sprintf("resolver-batch-%02d", index)
		if _, err := enrollNode(h, name, "central"); err != nil {
			return err
		}
	}
	midPlan, midRevision, err := createActivePlan(h, "mid-batch.example", 2026100603, 10, 1, 50)
	if err != nil {
		return err
	}
	midGroup, err := allocate(h, midPlan.ID, "batch-lease-mid01", "central", 10)
	if err != nil {
		return err
	}
	midItems := make([]domain.ProbeBatchItemInput, 0, 10)
	for index, leaseID := range midGroup.LeaseIDs {
		midItems = append(midItems, probeBatchItem(h, leaseID,
			fmt.Sprintf("batch-mid-%04d", index+1), midRevision.Serial, 8+index))
	}
	midJob, err := createProbeBatch(h, midPlan.ID, "batch-job-mid01", midItems)
	if err != nil {
		return err
	}
	sawInterrupted := false
	for attempt := 0; attempt < 40; attempt++ {
		window := 2*time.Millisecond + time.Duration(attempt*3)*time.Millisecond
		runCtx, runCancel := context.WithTimeout(context.Background(), window)
		_ = h.callCtx(runCtx, http.MethodPatch, "/v1/probe-batches/"+midJob.ID+"/confirm", nil, nil)
		runCancel()

		state, err := getProbeBatch(h, midJob.ID)
		if err != nil {
			return err
		}
		// Checkpoint must never exceed the number of terminal items.
		terminal := 0
		for _, item := range state.Items {
			if item.Outcome != domain.ProbeItemNotExecuted {
				terminal++
			}
		}
		if state.Checkpoint != terminal {
			return ensure(false, "checkpoint drifted ahead of executed items")
		}
		if state.State == domain.ProbeBatchInterrupted && state.Checkpoint > 0 {
			sawInterrupted = true
		}
		if state.State == domain.ProbeBatchCompleted {
			break
		}
	}
	midFinal, err := resumeProbeBatch(h, midJob.ID)
	if err != nil {
		return err
	}
	if midFinal.State != domain.ProbeBatchCompleted || midFinal.Checkpoint != 10 {
		return ensure(false, "interrupted batch did not resume to completion")
	}
	if !sawInterrupted {
		// Timing-dependent; tolerate environments that ran the whole batch per
		// call, but keep the zero-checkpoint cancel case covered above.
		fmt.Println("  (mid-batch interrupt not observed within the timing window; zero-checkpoint cancel covered)")
	}
	probeIDs := make(map[string]struct{}, 10)
	for _, item := range midFinal.Items {
		if item.Outcome != domain.ProbeItemSuccess || item.ProbeID == "" {
			return ensure(false, "mid-batch item did not succeed")
		}
		if _, duplicate := probeIDs[item.ProbeID]; duplicate {
			return ensure(false, "resume double-wrote a probe")
		}
		probeIDs[item.ProbeID] = struct{}{}
	}
	if len(probeIDs) != 10 {
		return ensure(false, "resume did not preserve exactly one probe per item")
	}

	// Intake rejections: unknown lease, intra-batch duplicate, invalid
	// payload, foreign lease, unknown plan.
	expectValidationFailure := func(err error, message string) error {
		if err == nil {
			return ensure(false, message)
		}
		return expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed))
	}
	bad := []domain.ProbeBatchItemInput{
		probeBatchItem(h, "lse_missing", "batch-prb-0007", revision.Serial, 10),
	}
	_, err = createProbeBatch(h, plan.ID, "batch-job-bad01", bad)
	if err := expectValidationFailure(err, "unknown lease was accepted"); err != nil {
		return err
	}
	dup := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseE, "batch-prb-0008", revision.Serial, 10),
		probeBatchItem(h, leaseE, "batch-prb-0009", revision.Serial, 10),
	}
	_, err = createProbeBatch(h, plan.ID, "batch-job-bad02", dup)
	if err := expectValidationFailure(err, "duplicate lease in batch was accepted"); err != nil {
		return err
	}
	invalidPayload := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseE, "batch-prb-0010", revision.Serial, 999999),
	}
	_, err = createProbeBatch(h, plan.ID, "batch-job-bad03", invalidPayload)
	if err := expectValidationFailure(err, "invalid payload was accepted"); err != nil {
		return err
	}

	otherPlan, _, err := createActivePlan(h, "other-batch.example", 2026100602, 1, 1, 50)
	if err != nil {
		return err
	}
	foreign := []domain.ProbeBatchItemInput{
		probeBatchItem(h, leaseE, "batch-prb-0011", revision.Serial, 10),
	}
	_, err = createProbeBatch(h, otherPlan.ID, "batch-job-bad04", foreign)
	if err := expectValidationFailure(err, "foreign lease was accepted"); err != nil {
		return err
	}
	_, err = createProbeBatch(h, "pln_missing", "batch-job-bad05", bad)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// The ordinary single-probe entry still works end to end: allocate one
	// more lease and submit directly, then replay to the same probe id.
	singleGroup, err := allocate(h, plan.ID, "batch-lease-0003", "central", 1)
	if err != nil {
		return err
	}
	first, err := submitProbe(h, singleGroup.LeaseIDs[0], "single-prb-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	replayed, err := submitProbe(h, singleGroup.LeaseIDs[0], "single-prb-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if replayed.ID != first.ID {
		return ensure(false, "single-probe replay returned a different item")
	}
	return nil
}
