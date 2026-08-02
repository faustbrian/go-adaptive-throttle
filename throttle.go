// Package throttle provides process-local probabilistic load shedding.
package throttle

import (
	"cmp"
	"context"
	"errors"
	"math"
	"math/bits"
	"slices"
	"sync"
	"time"
)

// ErrRejected reports that work was shed locally and must not be executed.
var ErrRejected = errors.New("adaptive throttle: rejected locally")

// Outcome is the mutually exclusive classification of one execution result.
type Outcome uint8

const (
	Accepted Outcome = iota
	DownstreamOverload
	DownstreamFailure
	Ignored
	LocalRejection
)

// Reason is a bounded classifier reason suitable for low-cardinality metrics.
type Reason uint8

const (
	ReasonUnspecified Reason = iota
	ReasonSuccess
	ReasonExplicitOverload
	ReasonDownstreamFailure
	ReasonCallerCanceled
	ReasonCallerDeadline
	ReasonLocalPolicy
)

// Classification is a bounded, caller-controlled result classification.
type Classification struct {
	Outcome Outcome
	Reason  Reason
}

// Decision is the admission result represented by an event.
type Decision uint8

const (
	DecisionRecorded Decision = iota
	DecisionAdmit
	DecisionReject
	DecisionDryRunAdmit
	DecisionReset
)

// Event is a bounded immutable observation. It intentionally exposes a numeric
// resource slot rather than the caller's arbitrary resource identity.
type Event struct {
	Decision     Decision
	Outcome      Outcome
	Reason       Reason
	Probability  float64
	Priority     uint8
	ResourceSlot uint64
	Snapshot     Snapshot
}

// Snapshot is an immutable aggregate for one bounded resource history.
type Snapshot struct {
	Revision             string
	Requests             uint64
	Accepts              uint64
	Samples              uint64
	Overloads            uint64
	Failures             uint64
	Ignored              uint64
	LocalRejections      uint64
	DryRunRejections     uint64
	RejectionProbability float64
	WindowAge            time.Duration
	ResourceSlot         uint64
}

type bucket struct {
	tick             int64
	requests         uint64
	accepts          uint64
	samples          uint64
	overloads        uint64
	failures         uint64
	ignored          uint64
	localRejections  uint64
	dryRunRejections uint64
}

type resourceState struct {
	buckets  []bucket
	lastTick int64
	lastTime time.Time
	lastUsed uint64
	slot     uint64
}

// Throttler owns bounded mutable process-local histories. It is safe for concurrent use.
type Throttler struct {
	policy    policyConfig
	mu        sync.Mutex
	resources map[string]*resourceState
	sequence  uint64
	slots     []bool
}

// New constructs an empty throttler from an immutable validated policy.
func New(policy Policy) (*Throttler, error) {
	if policy.config.revision == "" || policy.config.clock == nil || policy.config.random == nil || policy.config.bucketDuration <= 0 || policy.config.bucketCount < 1 || policy.config.maxResources < 1 {
		return nil, invalid("Policy", "must be created by NewPolicy")
	}
	return &Throttler{
		policy:    policy.config,
		resources: make(map[string]*resourceState),
		slots:     make([]bool, policy.config.maxResources+1),
	}, nil
}

// Record records one already-attempted result. Local rejection is retained as
// an application request but never as a downstream sample or failure.
func (t *Throttler) Record(resource string, classification Classification) error {
	if err := validateResource(resource); err != nil {
		return err
	}
	if !validOutcome(classification.Outcome) {
		return invalid("Classification.Outcome", "is unsupported")
	}
	now := t.policy.clock.Now()
	t.mu.Lock()
	state := t.resourceLocked(resource, now)
	b := t.currentBucketLocked(state, now)
	recordDirect(b, classification.Outcome)
	snapshot := aggregate(state, t.policy, now)
	t.mu.Unlock()
	t.observe(Event{Decision: DecisionRecorded, Outcome: classification.Outcome, Reason: classification.Reason, Probability: snapshot.RejectionProbability, ResourceSlot: state.slot, Snapshot: snapshot})
	return nil
}

// TryAcquire probabilistically admits work using history preceding this request.
// A nil permit with ErrRejected means the protected operation must not run.
func (t *Throttler) TryAcquire(ctx context.Context, resource string) (*Permit, error) {
	if ctx == nil {
		return nil, invalid("Context", "must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateResource(resource); err != nil {
		return nil, err
	}
	now := t.policy.clock.Now()
	sample := safeRandom(t.policy.random)
	priority := safePriority(t.policy.priority, ctx, len(t.policy.priorityScale))

	t.mu.Lock()
	state := t.resourceLocked(resource, now)
	b := t.currentBucketLocked(state, now)
	current := aggregate(state, t.policy, now)
	probability := current.RejectionProbability
	if len(t.policy.priorityScale) > 0 {
		probability *= t.policy.priorityScale[priority]
	}
	wouldReject := sample < probability
	if wouldReject && !t.policy.dryRun {
		increment(&b.requests)
		increment(&b.localRejections)
		snapshot := aggregate(state, t.policy, now)
		t.mu.Unlock()
		t.observe(Event{Decision: DecisionReject, Outcome: LocalRejection, Reason: ReasonLocalPolicy, Probability: probability, Priority: uint8(priority), ResourceSlot: state.slot, Snapshot: snapshot})
		return nil, ErrRejected
	}
	decision := DecisionAdmit
	if wouldReject {
		increment(&b.dryRunRejections)
		decision = DecisionDryRunAdmit
	}
	snapshot := aggregate(state, t.policy, now)
	permit := &Permit{owner: t, resource: resource, state: state}
	t.mu.Unlock()
	t.observe(Event{Decision: decision, Probability: probability, Priority: uint8(priority), ResourceSlot: state.slot, Snapshot: snapshot})
	return permit, nil
}

// Permit represents one admitted execution and accepts at most one completion.
type Permit struct {
	owner    *Throttler
	resource string
	state    *resourceState
	mu       sync.Mutex
	recorded bool
}

// Record records this admitted execution once.
func (p *Permit) Record(classification Classification) error {
	if p == nil || p.owner == nil {
		return invalid("Permit", "must not be nil")
	}
	if !validOutcome(classification.Outcome) || classification.Outcome == LocalRejection {
		return invalid("Classification.Outcome", "is invalid for admitted work")
	}
	if !p.record(classification) {
		return invalid("Permit", "was already recorded")
	}
	return nil
}

// Execute acquires before invoking operation and records its classified result
// exactly once. A locally rejected execution never invokes operation.
func Execute[T any](ctx context.Context, throttler *Throttler, resource string, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	if throttler == nil {
		return zero, invalid("Throttler", "must not be nil")
	}
	if operation == nil {
		return zero, invalid("Operation", "must not be nil")
	}
	permit, err := throttler.TryAcquire(ctx, resource)
	if err != nil {
		return zero, err
	}
	result, operationErr := operation(ctx)
	classification := safeClassify(throttler.policy.classifier, Completion{Context: ctx, Result: result, Err: operationErr})
	_ = permit.record(classification)
	return result, operationErr
}

func (p *Permit) record(classification Classification) bool {
	p.mu.Lock()
	if p.recorded {
		p.mu.Unlock()
		return false
	}
	p.recorded = true
	p.mu.Unlock()

	t := p.owner
	now := t.policy.clock.Now()
	t.mu.Lock()
	state, ok := t.resources[p.resource]
	if ok && state == p.state {
		b := t.currentBucketLocked(state, now)
		recordDirect(b, classification.Outcome)
		snapshot := aggregate(state, t.policy, now)
		t.mu.Unlock()
		t.observe(Event{Decision: DecisionRecorded, Outcome: classification.Outcome, Reason: classification.Reason, Probability: snapshot.RejectionProbability, ResourceSlot: state.slot, Snapshot: snapshot})
		return true
	}
	t.mu.Unlock()
	return true
}

// Snapshot returns an immutable aggregate without changing eviction order.
func (t *Throttler) Snapshot(resource string) (Snapshot, bool) {
	if validateResource(resource) != nil {
		return Snapshot{}, false
	}
	now := t.policy.clock.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.resources[resource]
	if !ok {
		return Snapshot{}, false
	}
	t.currentBucketLocked(state, now)
	return aggregate(state, t.policy, now), true
}

// Snapshots returns at most MaxResources immutable snapshots ordered by their
// stable process-local slot. Arbitrary resource identities are not exposed.
func (t *Throttler) Snapshots() []Snapshot {
	now := t.policy.clock.Now()
	t.mu.Lock()
	snapshots := make([]Snapshot, 0, len(t.resources))
	for _, state := range t.resources {
		t.currentBucketLocked(state, now)
		snapshots = append(snapshots, aggregate(state, t.policy, now))
	}
	t.mu.Unlock()
	slices.SortFunc(snapshots, func(left, right Snapshot) int {
		return cmp.Compare(left.ResourceSlot, right.ResourceSlot)
	})
	return snapshots
}

// Reset removes one resource history. Outstanding permits for it become inert.
func (t *Throttler) Reset(resource string) bool {
	if validateResource(resource) != nil {
		return false
	}
	now := t.policy.clock.Now()
	t.mu.Lock()
	state, ok := t.resources[resource]
	if !ok {
		t.mu.Unlock()
		return false
	}
	t.currentBucketLocked(state, now)
	snapshot := aggregate(state, t.policy, now)
	delete(t.resources, resource)
	t.slots[state.slot] = false
	t.mu.Unlock()
	t.observe(Event{Decision: DecisionReset, ResourceSlot: state.slot, Snapshot: snapshot})
	return true
}

// ResetAll removes every retained history and returns the number removed.
func (t *Throttler) ResetAll() int {
	t.mu.Lock()
	count := len(t.resources)
	t.resources = make(map[string]*resourceState)
	clear(t.slots)
	t.mu.Unlock()
	return count
}

func (t *Throttler) resourceLocked(resource string, now time.Time) *resourceState {
	if t.sequence != math.MaxUint64 {
		t.sequence++
	}
	if state, ok := t.resources[resource]; ok {
		state.lastUsed = t.sequence
		return state
	}
	if len(t.resources) == t.policy.maxResources {
		var victim string
		var victimState *resourceState
		for key, state := range t.resources {
			if victimState == nil || resourcePrecedes(state, victimState) {
				victim, victimState = key, state
			}
		}
		if victimState != nil {
			t.slots[victimState.slot] = false
			delete(t.resources, victim)
		}
	}
	slot := t.availableSlotLocked()
	t.slots[slot] = true
	tick := windowTick(now, t.policy.bucketDuration)
	state := &resourceState{
		buckets:  make([]bucket, t.policy.bucketCount),
		lastTick: tick,
		lastTime: now,
		lastUsed: t.sequence,
		slot:     slot,
	}
	state.buckets[bucketIndex(tick, t.policy.bucketCount)].tick = tick
	t.resources[resource] = state
	return state
}

func resourcePrecedes(left, right *resourceState) bool {
	return cmp.Or(
		cmp.Compare(left.lastUsed, right.lastUsed),
		cmp.Compare(left.slot, right.slot),
	) < 0
}

func (t *Throttler) availableSlotLocked() uint64 {
	for slot := 1; slot < len(t.slots); slot++ {
		if !t.slots[slot] {
			return uint64(slot)
		}
	}
	return 0
}

func (t *Throttler) currentBucketLocked(state *resourceState, now time.Time) *bucket {
	tick := windowTick(now, t.policy.bucketDuration)
	if now.Before(state.lastTime) || tick < state.lastTick || forwardGapAtLeast(state.lastTick, tick, t.policy.bucketCount) {
		clear(state.buckets)
	} else {
		for index := range state.buckets {
			retained := &state.buckets[index]
			if bucketHasData(retained) && forwardGapAtLeast(retained.tick, tick, t.policy.bucketCount) {
				*retained = bucket{}
			}
		}
	}
	state.lastTick = tick
	state.lastTime = now
	index := bucketIndex(tick, t.policy.bucketCount)
	if state.buckets[index].tick != tick {
		state.buckets[index] = bucket{tick: tick}
	}
	return &state.buckets[index]
}

func aggregate(state *resourceState, policy policyConfig, now time.Time) Snapshot {
	snapshot := Snapshot{Revision: policy.revision, ResourceSlot: state.slot}
	currentTick := windowTick(now, policy.bucketDuration)
	var oldestAge uint64
	for i := range state.buckets {
		b := &state.buckets[i]
		saturatingAdd(&snapshot.Requests, b.requests)
		saturatingAdd(&snapshot.Accepts, b.accepts)
		saturatingAdd(&snapshot.Samples, b.samples)
		saturatingAdd(&snapshot.Overloads, b.overloads)
		saturatingAdd(&snapshot.Failures, b.failures)
		saturatingAdd(&snapshot.Ignored, b.ignored)
		saturatingAdd(&snapshot.LocalRejections, b.localRejections)
		saturatingAdd(&snapshot.DryRunRejections, b.dryRunRejections)
		if bucketHasData(b) {
			oldestAge = max(oldestAge, uint64(currentTick)-uint64(b.tick))
		}
	}
	snapshot.WindowAge = time.Duration(oldestAge) * policy.bucketDuration
	snapshot.RejectionProbability = rejectionProbability(snapshot, policy)
	return snapshot
}

func bucketHasData(b *bucket) bool {
	return b.requests != 0 || b.accepts != 0 || b.samples != 0 || b.overloads != 0 || b.failures != 0 || b.ignored != 0 || b.localRejections != 0 || b.dryRunRejections != 0
}

func rejectionProbability(snapshot Snapshot, policy policyConfig) float64 {
	if snapshot.Samples < policy.minimumSamples || snapshot.Requests == 0 {
		return 0
	}
	requests := float64(snapshot.Requests)
	accepts := float64(snapshot.Accepts)
	probability := (requests - policy.acceptsK*accepts) / (requests + 1)
	if !finite(probability) || math.Signbit(probability) {
		return 0
	}
	if probability >= policy.maxProbability {
		return math.Nextafter(policy.maxProbability, 0)
	}
	return probability
}

func recordDirect(b *bucket, outcome Outcome) {
	switch outcome {
	case Accepted:
		increment(&b.requests)
		increment(&b.accepts)
		increment(&b.samples)
	case DownstreamOverload:
		increment(&b.requests)
		increment(&b.samples)
		increment(&b.overloads)
	case DownstreamFailure:
		increment(&b.requests)
		increment(&b.accepts)
		increment(&b.samples)
		increment(&b.failures)
	case Ignored:
		increment(&b.ignored)
	case LocalRejection:
		increment(&b.requests)
		increment(&b.localRejections)
	}
}

func increment(value *uint64) {
	if *value != math.MaxUint64 {
		*value++
	}
}

func saturatingAdd(total *uint64, value uint64) {
	sum, carry := bits.Add64(*total, value, 0)
	if carry != 0 {
		*total = math.MaxUint64
		return
	}
	*total = sum
}

func validOutcome(outcome Outcome) bool { return outcome <= LocalRejection }

func validateResource(resource string) error {
	if resource == "" {
		return invalid("Resource", "must not be empty")
	}
	if len(resource) > MaxResourceBytes {
		return invalid("Resource", "exceeds maximum length")
	}
	return nil
}

func safeRandom(random Random) (sample float64) {
	sample = 1
	defer func() {
		if recover() != nil {
			sample = 1
		}
	}()
	value := random.Float64()
	if finite(value) && !math.Signbit(value) && math.Signbit(value-1) {
		return value
	}
	return 1
}

func safeClassify(classifier Classifier, completion Completion) (classification Classification) {
	classification = Classification{Outcome: Ignored, Reason: ReasonUnspecified}
	defer func() {
		if recover() != nil {
			classification = Classification{Outcome: Ignored, Reason: ReasonUnspecified}
		}
	}()
	classified := classifier(completion)
	if !validOutcome(classified.Outcome) || classified.Outcome == LocalRejection {
		return classification
	}
	return classified
}

func safePriority(resolver PriorityResolver, ctx context.Context, levels int) (priority Priority) {
	if nilInterface(resolver) {
		return 0
	}
	defer func() {
		if recover() != nil {
			priority = 0
		}
	}()
	priority = resolver(ctx)
	if int(priority) >= levels {
		return 0
	}
	return priority
}

func (t *Throttler) observe(event Event) {
	if t.policy.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	t.policy.observer(event)
}

func windowTick(now time.Time, duration time.Duration) int64 {
	nanoseconds := now.UnixNano()
	divisor := int64(duration)
	quotient := nanoseconds / divisor
	if nanoseconds>>63 != 0 && nanoseconds%divisor != 0 {
		quotient--
	}
	return quotient
}

func bucketIndex(tick int64, count int) int {
	index := tick % int64(count)
	if index < 0 {
		index += int64(count)
	}
	return int(index)
}

func forwardGapAtLeast(previous, current int64, count int) bool {
	if cmp.Compare(current, previous) != 1 {
		return false
	}
	return uint64(current)-uint64(previous) >= uint64(count)
}
