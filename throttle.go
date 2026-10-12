// Package throttle provides process-local probabilistic load shedding from
// rolling requests-versus-accepts history. It is not a hard quota, concurrency
// limit, authorization control or circuit breaker. Empty/sparse history has zero
// shedding probability;
// expected recovery probe flow is not a guaranteed count in a finite interval.
//
// Construct a validated Policy, then an independent Throttler. TryAcquire
// admits before work; its caller-owned Permit records one completion. Execute
// performs this sequence synchronously. Ordinary failures count as accepted
// downstream work; the default classifier ignores all errors unless the caller
// supplies explicit evidence policy.
//
// Configuration strings/scales are copied, but collaborators and closure state
// remain caller-owned and must be bounded/concurrency-safe. Callbacks run
// outside state locks, can overlap and cannot be preempted by cancellation. The
// package starts no goroutines and has no Close/drain operation. Reset discards
// history, not downstream work or caller-held permits.
package throttle

import (
	"cmp"
	"context"
	"errors"
	"math"
	"math/bits"
	"slices"
	"strings"
	"sync"
	"time"
)

// ErrRejected reports that work was shed locally and must not be executed.
var ErrRejected = errors.New("adaptive throttle: rejected locally")

// Outcome is the mutually exclusive classification of one execution result.
type Outcome uint8

const (
	// Accepted counts a successful downstream completion as one request, accept
	// and sample.
	Accepted Outcome = iota
	// DownstreamOverload counts explicit overload as one request/sample, not an
	// accept.
	DownstreamOverload
	// DownstreamFailure counts an ordinary failed completion as one request,
	// accept and sample.
	DownstreamFailure
	// Ignored counts an excluded observation without changing requests, accepts or
	// samples.
	Ignored
	// LocalRejection counts a locally shed application request, never a downstream
	// sample. It is invalid for Permit.Record and classifier output.
	LocalRejection
)

// Reason is a bounded classifier reason suitable for low-cardinality metrics.
type Reason uint8

const (
	// ReasonUnspecified denotes no selected classification reason.
	ReasonUnspecified Reason = iota
	// ReasonSuccess denotes a successful operation.
	ReasonSuccess
	// ReasonExplicitOverload denotes proven downstream overload evidence.
	ReasonExplicitOverload
	// ReasonDownstreamFailure denotes an ordinary completed downstream failure.
	ReasonDownstreamFailure
	// ReasonCallerCanceled denotes caller cancellation excluded from history
	// accounting.
	ReasonCallerCanceled
	// ReasonCallerDeadline denotes caller deadline exhaustion excluded from
	// history accounting.
	ReasonCallerDeadline
	// ReasonLocalPolicy denotes local-policy rejection, not downstream overload
	// evidence.
	ReasonLocalPolicy
)

// Classification identifies one result. Outcome is validated; Reason is a
// bounded caller-owned byte and is not restricted to the named constants.
type Classification struct {
	// Outcome selects request/accept/sample accounting; zero is Accepted.
	Outcome Outcome
	// Reason is caller-provided classification metadata, not a predicate for
	// accounting.
	Reason Reason
}

// Decision is the admission result represented by an event.
type Decision uint8

const (
	// DecisionRecorded observes a completed manual or permit record.
	DecisionRecorded Decision = iota
	// DecisionAdmit observes admission before downstream completion.
	DecisionAdmit
	// DecisionReject observes a locally rejected attempt.
	DecisionReject
	// DecisionDryRunAdmit observes a would-reject admission in dry run.
	DecisionDryRunAdmit
	// DecisionReset observes one removed history after expiry; ResetAll emits no
	// event.
	DecisionReset
)

// Event is a copied observation without resource identities, operation payloads
// or errors. Its Snapshot reflects the recorded state transition; Probability
// can be the priority-adjusted decision value. Slots are reusable, not
// permanent identities, and concurrent callback delivery need not follow
// transition order.
type Event struct {
	// Decision distinguishes admission, recording and reset event semantics.
	Decision Decision
	// Outcome describes recorded/rejected work. Admit/reset events leave its zero
	// value; this is not a downstream-success observation.
	Outcome Outcome
	// Reason is classification metadata for recorded/rejected work; other events
	// leave it unspecified.
	Reason Reason
	// Probability is priority-adjusted for admission decisions and unscaled for
	// recorded results. Reset events leave it zero; Snapshot contains aggregate
	// probability.
	Probability float64
	// Priority is the resolved level for admission events; other events leave it
	// zero.
	Priority uint8
	// ResourceSlot is in [1, configured MaxResources] while active and is reused
	// after reset/eviction.
	ResourceSlot uint64
	// Snapshot is the copied aggregate at this state transition. Admission events
	// precede completion; recorded events include the new observation.
	Snapshot Snapshot
}

// Snapshot is a copied aggregate for one active rolling history. Counters
// saturate at MaxUint64; an admission does not add a downstream sample until
// completion. Reading a snapshot expires stale buckets but does not refresh
// eviction order.
type Snapshot struct {
	// Revision is the copied bounded policy label, not a resource identity.
	Revision string
	// Requests counts Accepted, DownstreamOverload, DownstreamFailure and
	// LocalRejection, not Ignored.
	Requests uint64
	// Accepts counts Accepted and ordinary DownstreamFailure, not only successful
	// work.
	Accepts uint64
	// Samples counts completed Accepted, DownstreamOverload and DownstreamFailure
	// observations.
	Samples uint64
	// Overloads counts explicit DownstreamOverload observations.
	Overloads uint64
	// Failures counts ordinary DownstreamFailure observations.
	Failures uint64
	// Ignored counts excluded observations without requests, accepts or samples.
	Ignored uint64
	// LocalRejections counts application attempts shed before downstream work.
	LocalRejections uint64
	// DryRunRejections counts would-reject admissions, independently of their
	// later completions.
	DryRunRejections uint64
	// RejectionProbability is the unscaled current probability before trusted
	// priority adjustment, zero below MinimumSamples and strictly below the
	// effective cap.
	RejectionProbability float64
	// WindowAge is bucket-granular age of the oldest nonempty retained bucket;
	// zero denotes no observations or observations only in the current bucket.
	WindowAge time.Duration
	// ResourceSlot identifies the active slot, reused after eviction/reset. It
	// does not identify a permanent logical resource.
	ResourceSlot uint64
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
	resource string
	buckets  []bucket
	lastTick int64
	lastTime time.Time
	lastUsed uint64
	slot     uint64
}

// Throttler owns bounded mutable process-local histories and is safe for
// concurrent use when its caller-owned collaborators are. Construct with New;
// nil/zero receivers are not usable throttlers. Do not copy a Throttler after
// use. The package owns no background goroutines, timers or shutdown resources;
// it does not limit in-flight work.
type Throttler struct {
	policy policyConfig
	// mu owns the resource map, slot allocation, eviction sequence and all buckets.
	// Caller collaborators run outside it; snapshots copy state before delivery.
	mu        sync.Mutex
	resources map[string]*resourceState
	sequence  uint64
	slots     []bool
}

// New constructs empty independent histories from a Policy created by
// NewPolicy, returning nil and *ConfigError for an invalid/zero policy.
// Collaborator references remain shared; no goroutine or asynchronous lifecycle
// is started.
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

// Record counts one already-attempted result without admission or
// classification callbacks. Every valid outcome, including LocalRejection, is
// accepted; invalid resource/outcome returns *ConfigError before history
// mutation. Ignored increments only its own counter; LocalRejection increments
// requests and local rejection but no downstream sample. Observation is
// synchronous after unlocking.
func (t *Throttler) Record(resource string, classification Classification) error {
	if err := validateResource(resource); err != nil {
		return err
	}
	if !validOutcome(classification.Outcome) {
		return invalid("Classification.Outcome", "is unsupported")
	}
	now := safeNow(t.policy.clock)
	t.mu.Lock()
	state := t.resourceLocked(resource, now)
	b := t.currentBucketLocked(state, now)
	recordDirect(b, classification.Outcome)
	snapshot := aggregate(state, t.policy, now)
	t.mu.Unlock()
	t.observe(Event{Decision: DecisionRecorded, Outcome: classification.Outcome, Reason: classification.Reason, Probability: snapshot.RejectionProbability, ResourceSlot: state.slot, Snapshot: snapshot})
	return nil
}

// TryAcquire uses history preceding this decision. A sample strictly
// below the priority-adjusted probability rejects; equality admits. Success
// returns a non-nil permit, while ErrRejected returns nil and forbids running
// protected work. Dry run admits and records the counterfactual instead.
// Resource keys must be nonempty and at most MaxResourceBytes bytes.
// Nil context/resource errors are *ConfigError; observed cancellation/deadline
// is returned directly before state mutation, including rechecks after injected
// admission collaborators and lock contention. Mutex waiting and synchronous
// collaborators are not preemptible. Cancellation after admission does not
// record or revoke the permit.
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
	now := safeNow(t.policy.clock)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sample := safeRandom(t.policy.random)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	priority := safePriority(t.policy.priority, ctx, len(t.policy.priorityScale))
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Capture external context methods outside the lock; after contention the
	// locked checkpoint reads only the channel and unlocks before calling Err.
	done := ctx.Done()
	t.mu.Lock()
	select {
	case <-done:
		t.mu.Unlock()
		return nil, ctx.Err()
	default:
	}
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
	permit := &Permit{owner: t, resource: state.resource, state: state}
	t.mu.Unlock()
	t.observe(Event{Decision: decision, Probability: probability, Priority: uint8(priority), ResourceSlot: state.slot, Snapshot: snapshot})
	return permit, nil
}

// Permit is one caller-owned admitted execution with at most one valid
// completion, safe for concurrent Record calls. Do not copy it after use. It
// retains its throttler, owned resource key and generation, even after
// recording/reset/eviction; complete and discard permits promptly. There is no
// release, timeout or automatic cancellation completion.
type Permit struct {
	owner    *Throttler
	resource string
	state    *resourceState
	mu       sync.Mutex
	recorded bool
}

// Record consumes one valid admitted completion. Nil/zero permits, invalid
// outcomes, LocalRejection or a second valid completion return *ConfigError.
// Invalid outcomes do not consume the permit. A reset/evicted generation
// consumes completion successfully but produces no history or event; it never
// recreates or merges a newer history.
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

// Execute admits before synchronously invoking operation once, then classifies
// and records its returned result/error. Rejection returns the zero T without
// invoking operation. It returns the operation's original result/error after
// admission and never retries, sleeps, recovers operation panics or forces
// cancellation. An operation panic bypasses its completion recording. Nil
// throttler/operation returns *ConfigError; other admission errors are
// propagated. Classifier and observer panics are contained by their own
// contracts.
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
	// The one-shot flag has its own owner and is released before caller clock
	// code or the history mutex, so a second completion cannot race into counts.
	p.mu.Lock()
	if p.recorded {
		p.mu.Unlock()
		return false
	}
	p.recorded = true
	p.mu.Unlock()

	t := p.owner
	now := safeNow(t.policy.clock)
	t.mu.Lock()
	// Slots and keys can be reused; pointer identity binds completion to the
	// admitted generation, preventing reset/eviction from resurrecting history.
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

// Snapshot expires buckets and returns a copied aggregate without creating
// history or changing eviction order. Invalid or missing resource returns a
// zero Snapshot and false. Expired but retained identities return true with
// cleared counters.
func (t *Throttler) Snapshot(resource string) (Snapshot, bool) {
	if validateResource(resource) != nil {
		return Snapshot{}, false
	}
	now := safeNow(t.policy.clock)
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.resources[resource]
	if !ok {
		return Snapshot{}, false
	}
	t.currentBucketLocked(state, now)
	return aggregate(state, t.policy, now), true
}

// Snapshots returns independent aggregates ordered by reusable numeric slot,
// not identity or recency. It scans every active resource and its buckets
// (bounded by the configured combined capacity), expires history and leaves
// eviction order unchanged. It does not expose keys or create new histories.
func (t *Throttler) Snapshots() []Snapshot {
	now := safeNow(t.policy.clock)
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

// Reset removes a valid existing resource and releases its slot for reuse,
// making outstanding permits inert. It emits a post-expiry snapshot event
// outside the lock. Invalid or missing resource returns false; this does not
// stop downstream work.
func (t *Throttler) Reset(resource string) bool {
	if validateResource(resource) != nil {
		return false
	}
	now := safeNow(t.policy.clock)
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

// ResetAll discards active histories and returns the count removed, making
// their permits inert. It emits no per-resource events and neither waits for
// nor cancels in-flight downstream work.
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
	ownedResource := strings.Clone(resource)
	state := &resourceState{
		resource: ownedResource,
		buckets:  make([]bucket, t.policy.bucketCount),
		lastTick: tick,
		lastTime: now,
		lastUsed: t.sequence,
		slot:     slot,
	}
	state.buckets[bucketIndex(tick, t.policy.bucketCount)].tick = tick
	t.resources[ownedResource] = state
	return state
}

// resourcePrecedes breaks recency ties by slot, including when the usage
// sequence saturates, so map iteration cannot choose a nondeterministic victim.
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
	// Timestamp reversal, even within a bucket, discards stale overload evidence.
	// Partial rotation also expires ignored/dry-run-only buckets for correct age.
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
			// #nosec G115 -- Rotation bounds populated buckets to a nonnegative age below the validated bucket count; unsigned subtraction preserves negative ticks.
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
	// Keep a representable value strictly below the cap; equality would lose
	// the strict probe-flow bound expected by decision and saturation tests.
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

func safeNow(clock Clock) (now time.Time) {
	defer func() {
		if recover() != nil {
			now = time.Now()
		}
	}()

	return clock.Now()
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
	// Go division truncates toward zero; ring windows need floor division so
	// sub-second timestamps before the epoch stay in the preceding bucket.
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
	// #nosec G115 -- Signed ordering makes unsigned subtraction the exact full-domain forward gap; policy validation bounds count to 1..MaxBuckets.
	return uint64(current)-uint64(previous) >= uint64(count)
}
