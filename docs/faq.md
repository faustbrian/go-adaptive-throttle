# FAQ

## Is this a circuit breaker?

No. It probabilistically sheds a bounded share while retaining probes. A
circuit breaker has explicit open and recovery states and may reject all work.

## Is the limit shared across pods?

No. State and random decisions are local to one process. Use a real distributed
rate or concurrency control when a fleet-wide bound is required.

## Why do local rejections increase requests?

That is part of the Google SRE equation: application demand continues to be
visible while the client self-regulates. Local rejection never increments
downstream samples, overloads, or failures.

## Why are generic errors ignored?

An error alone does not prove that downstream work ran. It may come from a rate
limit, bulkhead, breaker, retry wrapper, caller budget, or another local policy.
Treat proven completed work as `DownstreamFailure`, and map only explicit,
service-owned overload signals to `DownstreamOverload`.

## Why did a new pod admit more work?

It started with an empty local window. Plan cold-start aggregate admission at
the maximum replica count and use readiness or application-owned ramping when
needed.

## Can critical traffic bypass every control?

No. Priority only scales adaptive rejection. Hard bulkhead, rate, authorization,
and concurrency controls still apply.

## How is policy reconfigured?

Create a new immutable policy and throttler. This resets local history. Run
revisions side by side only when the application explicitly owns that rollout.
