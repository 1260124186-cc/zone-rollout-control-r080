package workflowprobe

import (
	"fmt"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runProbeBatchJob() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	for index, region := range []string{"batch", "batch", "batch", "batchttl", "batchfresh", "batchconc", "batchconc"} {
		name := fmt.Sprintf("resolver-batch-%02d", index+1)
		if _, err := enrollNode(harness, name, region); err != nil {
			return err
		}
	}
	plan, revision, err := createActivePlan(harness, "batch.example", 7001, 3, 0.5, 500)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "batch-lease-0001", "batch", 3)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 3 {
		return ensure(false, "batch plan must hold three leases")
	}
	lease0, lease1, lease2 := group.LeaseIDs[0], group.LeaseIDs[1], group.LeaseIDs[2]

	observedAt := harness.clock.Now()
	pre, err := submitProbe(harness, lease0, "batch-pre-0001", true, revision.Serial, 10)
	if err != nil {
		return err
	}

	batchInput := domain.ProbeBatchInput{
		RequestKey: "batch-job-0001",
		Items: []domain.ProbeBatchItemInput{
			{LeaseID: lease0, RequestKey: "batch-pre-0001", Success: true, ObservedSerial: revision.Serial, LatencyMS: 10, ObservedAt: observedAt},
			{LeaseID: lease1, RequestKey: "batch-item-0002", Success: true, ObservedSerial: revision.Serial, LatencyMS: 20, ObservedAt: observedAt},
			{LeaseID: lease2, RequestKey: "batch-item-0003", Success: false, ObservedSerial: revision.Serial, LatencyMS: 30, ObservedAt: observedAt},
			{LeaseID: lease1, RequestKey: "batch-item-0004", Success: true, ObservedSerial: revision.Serial, LatencyMS: 40, ObservedAt: observedAt},
			{LeaseID: "lse_missing0000", RequestKey: "batch-item-0005", Success: true, ObservedSerial: revision.Serial, LatencyMS: 50, ObservedAt: observedAt},
			{LeaseID: lease2, RequestKey: "batch-item-0006", Success: true, ObservedSerial: revision.Serial, LatencyMS: 700000, ObservedAt: observedAt},
			{LeaseID: "", RequestKey: "batch-item-0007", Success: true, ObservedSerial: revision.Serial, LatencyMS: 70, ObservedAt: observedAt},
		},
	}
	var batch domain.ProbeBatch
	status, err := harness.callStatus(http.MethodPut, "/v1/probe-batches", batchInput, &batch)
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return ensure(false, "first batch submit must return 201")
	}
	if batch.State != domain.ProbeBatchCommitted || batch.Total != 7 || batch.CommittedCount != 7 {
		return ensure(false, "batch must finish committed with all seven items decided")
	}
	if batch.FinishedAt == nil {
		return ensure(false, "committed batch must record finished_at")
	}
	if err := ensureCommitOrder(batch); err != nil {
		return err
	}
	if err := ensurePositions(batch, 7); err != nil {
		return err
	}

	expectations := []struct {
		key       string
		outcome   domain.ProbeBatchItemOutcome
		errorCode string
	}{
		{"batch-pre-0001", domain.ProbeBatchItemReplayed, ""},
		{"batch-item-0002", domain.ProbeBatchItemStored, ""},
		{"batch-item-0003", domain.ProbeBatchItemStored, ""},
		{"batch-item-0004", domain.ProbeBatchItemConflict, string(domain.CodeConflict)},
		{"batch-item-0005", domain.ProbeBatchItemNotExecuted, string(domain.CodeNotFound)},
		{"batch-item-0006", domain.ProbeBatchItemNotExecuted, string(domain.CodeValidationFailed)},
		{"batch-item-0007", domain.ProbeBatchItemNotExecuted, string(domain.CodeValidationFailed)},
	}
	for _, expectation := range expectations {
		item, err := batchItemByKey(batch, expectation.key)
		if err != nil {
			return err
		}
		if item.Outcome != expectation.outcome {
			return ensure(false, "batch item "+expectation.key+" has unexpected outcome")
		}
		if expectation.errorCode == "" && item.Error != nil {
			return ensure(false, "batch item "+expectation.key+" must not carry an error")
		}
		if expectation.errorCode != "" {
			if item.Error == nil || item.Error.Code != expectation.errorCode {
				return ensure(false, "batch item "+expectation.key+" has unexpected error")
			}
		}
	}
	replayed, err := batchItemByKey(batch, "batch-pre-0001")
	if err != nil {
		return err
	}
	if replayed.ProbeID != pre.ID {
		return ensure(false, "batch replay must reference the single-endpoint probe")
	}
	stored, err := batchItemByKey(batch, "batch-item-0002")
	if err != nil {
		return err
	}
	if stored.ProbeID == "" {
		return ensure(false, "stored batch item must reference its probe")
	}

	var retry domain.ProbeBatch
	status, err = harness.callStatus(http.MethodPut, "/v1/probe-batches", batchInput, &retry)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return ensure(false, "batch retry must return 200")
	}
	if retry.ID != batch.ID {
		return ensure(false, "batch retry returned another job")
	}
	if err := ensureSameOutcomes(batch, retry); err != nil {
		return err
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "batch-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.ProbeCount != 3 || assessment.SuccessCount != 2 {
		return ensure(false, "batch retry wrote duplicate probes")
	}

	conflicting := batchInput
	conflicting.Items = batchInput.Items[:1]
	err = harness.call(http.MethodPut, "/v1/probe-batches", conflicting, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	var fetched domain.ProbeBatch
	if err := harness.call(http.MethodGet, "/v1/probe-batches/"+batch.ID, nil, &fetched); err != nil {
		return err
	}
	if err := ensureSameOutcomes(batch, fetched); err != nil {
		return err
	}
	err = harness.call(http.MethodGet, "/v1/probe-batches/pbt_missing", nil, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	closure, err := submitProbe(harness, lease1, "batch-item-0002", true, revision.Serial, 20)
	if err != nil {
		return err
	}
	if closure.ID != stored.ProbeID {
		return ensure(false, "single endpoint must replay the batch-stored probe")
	}
	_, err = submitProbe(harness, lease1, "batch-item-0002", true, revision.Serial, 25)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	err = harness.call(http.MethodPut, "/v1/probe-batches", domain.ProbeBatchInput{
		RequestKey: "batch-job-0002",
		Items:      []domain.ProbeBatchItemInput{},
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	err = harness.call(http.MethodPut, "/v1/probe-batches", domain.ProbeBatchInput{
		RequestKey: "x",
		Items:      batchInput.Items[:1],
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	shortGroup, err := allocateSeconds(harness, plan.ID, "batch-lease-0002", "batchttl", 30, 1)
	if err != nil {
		return err
	}
	freshGroup, err := allocate(harness, plan.ID, "batch-lease-0003", "batchfresh", 1)
	if err != nil {
		return err
	}
	concGroup, err := allocate(harness, plan.ID, "batch-lease-0004", "batchconc", 2)
	if err != nil {
		return err
	}
	harness.clock.Advance(31 * time.Second)

	expiredBatch, err := submitBatch(harness, "batch-job-0003", []domain.ProbeBatchItemInput{
		{LeaseID: shortGroup.LeaseIDs[0], RequestKey: "batch-item-0008", Success: true, ObservedSerial: revision.Serial, LatencyMS: 15, ObservedAt: harness.clock.Now()},
		{LeaseID: freshGroup.LeaseIDs[0], RequestKey: "batch-item-0009", Success: true, ObservedSerial: revision.Serial, LatencyMS: 25, ObservedAt: harness.clock.Now()},
	})
	if err != nil {
		return err
	}
	expiredItem, err := batchItemByKey(expiredBatch, "batch-item-0008")
	if err != nil {
		return err
	}
	if expiredItem.Outcome != domain.ProbeBatchItemExpired || expiredItem.Error == nil ||
		expiredItem.Error.Code != string(domain.CodeLeaseExpired) {
		return ensure(false, "expired lease must produce the expired outcome")
	}
	freshItem, err := batchItemByKey(expiredBatch, "batch-item-0009")
	if err != nil {
		return err
	}
	if freshItem.Outcome != domain.ProbeBatchItemStored || freshItem.ProbeID == "" {
		return ensure(false, "valid item beside an expired one must still store")
	}
	err = harness.call(http.MethodPatch, "/v1/leases/"+shortGroup.LeaseIDs[0]+"/probes", domain.ProbeInput{
		RequestKey:     "batch-item-0008",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      15,
		ObservedAt:     harness.clock.Now(),
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}

	concurrentInput := domain.ProbeBatchInput{
		RequestKey: "batch-job-0004",
		Items: []domain.ProbeBatchItemInput{
			{LeaseID: concGroup.LeaseIDs[0], RequestKey: "batch-item-0010", Success: true, ObservedSerial: revision.Serial, LatencyMS: 35, ObservedAt: harness.clock.Now()},
			{LeaseID: concGroup.LeaseIDs[1], RequestKey: "batch-item-0011", Success: true, ObservedSerial: revision.Serial, LatencyMS: 45, ObservedAt: harness.clock.Now()},
		},
	}
	type concurrentResult struct {
		status int
		batch  domain.ProbeBatch
		err    error
	}
	results := make(chan concurrentResult, 3)
	for i := 0; i < 3; i++ {
		go func() {
			var batch domain.ProbeBatch
			status, err := harness.callStatus(http.MethodPut, "/v1/probe-batches", concurrentInput, &batch)
			results <- concurrentResult{status: status, batch: batch, err: err}
		}()
	}
	created := 0
	var concurrentID string
	for i := 0; i < 3; i++ {
		result := <-results
		if result.err != nil {
			return result.err
		}
		if concurrentID == "" {
			concurrentID = result.batch.ID
		}
		if result.batch.ID != concurrentID {
			return ensure(false, "concurrent submits returned different batches")
		}
		if result.batch.State != domain.ProbeBatchCommitted || result.batch.CommittedCount != 2 {
			return ensure(false, "concurrent batch must finish committed")
		}
		if result.status == http.StatusCreated {
			created++
		}
	}
	if created != 1 {
		return ensure(false, "exactly one concurrent submit may create the batch")
	}

	var final domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "batch-assess-0002",
	}, &final); err != nil {
		return err
	}
	if final.ProbeCount != 6 || final.SuccessCount != 5 {
		return ensure(false, "concurrent retries wrote duplicate probes")
	}
	return nil
}

func submitBatch(harness *harness, requestKey string, items []domain.ProbeBatchItemInput) (domain.ProbeBatch, error) {
	var batch domain.ProbeBatch
	err := harness.call(http.MethodPut, "/v1/probe-batches", domain.ProbeBatchInput{
		RequestKey: requestKey,
		Items:      items,
	}, &batch)
	return batch, err
}

func allocateSeconds(
	harness *harness,
	planID string,
	requestKey string,
	region string,
	leaseSeconds int,
	maxNodes int,
) (domain.LeaseGroup, error) {
	var group domain.LeaseGroup
	err := harness.call(http.MethodPatch, "/v1/plans/"+planID+"/leases", domain.LeaseInput{
		RequestKey:   requestKey,
		Region:       region,
		LeaseSeconds: leaseSeconds,
		MaxNodes:     maxNodes,
	}, &group)
	return group, err
}

func batchItemByKey(batch domain.ProbeBatch, requestKey string) (domain.ProbeBatchItem, error) {
	for _, item := range batch.Items {
		if item.RequestKey == requestKey {
			return item, nil
		}
	}
	return domain.ProbeBatchItem{}, fmt.Errorf("batch item %q not found", requestKey)
}

func ensureCommitOrder(batch domain.ProbeBatch) error {
	for index := 1; index < len(batch.Items); index++ {
		previous := batch.Items[index-1]
		current := batch.Items[index]
		if previous.LeaseID > current.LeaseID {
			return ensure(false, "batch items are not in stable lease order")
		}
		if previous.LeaseID == current.LeaseID && previous.RequestKey > current.RequestKey {
			return ensure(false, "batch items are not in stable request key order")
		}
		if previous.LeaseID == current.LeaseID && previous.RequestKey == current.RequestKey &&
			previous.Position > current.Position {
			return ensure(false, "batch items are not in stable position order")
		}
	}
	return nil
}

func ensurePositions(batch domain.ProbeBatch, total int) error {
	seen := make(map[int]bool, total)
	for _, item := range batch.Items {
		if item.Position < 0 || item.Position >= total || seen[item.Position] {
			return ensure(false, "batch item positions must cover the submitted indexes")
		}
		seen[item.Position] = true
	}
	return nil
}

func ensureSameOutcomes(expected domain.ProbeBatch, actual domain.ProbeBatch) error {
	if expected.ID != actual.ID || expected.State != actual.State || expected.Total != actual.Total ||
		expected.CommittedCount != actual.CommittedCount {
		return ensure(false, "batch identity or checkpoint changed between reads")
	}
	for _, expectedItem := range expected.Items {
		actualItem, err := batchItemByKey(actual, expectedItem.RequestKey)
		if err != nil {
			return err
		}
		if actualItem.Outcome != expectedItem.Outcome || actualItem.ProbeID != expectedItem.ProbeID {
			return ensure(false, "batch item outcome changed between reads")
		}
	}
	return nil
}
