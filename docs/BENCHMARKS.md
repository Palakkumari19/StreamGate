# Benchmarks

> **Placeholder — written properly in Phase 7.** It will contain: the methodology (hardware,
> concurrency levels, duration, warm-up, number of runs), the k6 scenarios, a routing-strategy
> comparison table, and the raw output location.
>
> **No numbers are published anywhere in this repo until they come from a real measured run on
> stated hardware.** Fabricated or illustrative performance figures are worse than none,
> because they cannot be defended when questioned.

Planned scenarios:

1. Baseline throughput and TTFT percentiles, all backends healthy, equal speed.
2. Heterogeneous fleet (one backend 3× slower) — the case that separates round-robin from the
   load-aware strategies.
3. One backend failing (`error_rate: 1.0`) — measures breaker reaction time and client-visible
   error rate.
4. Rate-limit enforcement accuracy across 2 gateway replicas.
5. Real-backend sanity run against Ollama, to check that mock-based conclusions transfer
   ([R-18](RISKS.md)).
