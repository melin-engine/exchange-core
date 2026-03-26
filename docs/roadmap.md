# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Manual promotion | High | Medium | ★★★☆☆ | "How do I failover?" is a deal-breaker question. Admin command to promote replica. |
| 2 | Output event channel | High | Medium | ★★★☆☆ | Prerequisite for market data, audit trail, and replica analytics. Unlocks many downstream features. |
| 3 | GTD TIF | Low | Very low | ★★★☆☆ | Easy add, nice checkbox. Less asked-for than Day. |
| 4 | Per-account trading permissions | Medium | Medium | ★★★☆☆ | Multi-tenant deployments need account-level access control. |
| 5 | Order throttling | Medium | Low | ★★★☆☆ | SEC-04 audit finding. Simple per-account counter on the hot path. |
| 6 | Snapshot schedule | Medium | Low | ★★★☆☆ | Operators don't want to trigger snapshots manually. Timer + existing save logic. |
| 7 | Output event log | High | Medium | ★★★☆☆ | Regulatory requirement, but depends on output event channel first. |
| 8 | Reference data management | Medium | Medium | ★★★☆☆ | Instrument disable/remove. Operators expect lifecycle management. |
| 9 | Catch-up from journal files | High | High | ★★☆☆☆ | Critical for production HA, but significant work (read historical segments, switch to live). |
| 10 | TLS | Medium | Medium | ★★☆☆☆ | Some buyers require it (compliance). Most exchange deployments use VLAN instead. |
| 11 | Tiered fee schedules | Medium | Medium | ★★☆☆☆ | Nice-to-have — most buyers customize fees anyway. |
| 12 | Position/exposure limits | Medium | Medium | ★★☆☆☆ | Important for derivatives, less so for spot. |
| 13 | Market data dissemination | High | High | ★★☆☆☆ | High value but large scope. Depends on output event channel. |
| 14 | Iceberg orders | Low | Medium | ★★☆☆☆ | Niche. Only matters for venues with institutional flow. |
| 15 | Auction mechanisms | High | Very high | ★☆☆☆☆ | Differentiator for regulated venues, but massive complexity (state machine, indicative pricing, uncrossing). |
| 16 | Snapshot transfer | Medium | High | ★☆☆☆☆ | Needed for full HA, but catch-up from journal comes first. |
| 17 | Client failover | Medium | High | ★☆☆☆☆ | Client-side reconnect + sequence resume. Significant protocol work. |
| 18 | Failover detection + promotion | High | Very high | ★☆☆☆☆ | Leader election, split-brain — distributed systems hard mode. |
| 19 | Network partition handling | High | Very high | ★☆☆☆☆ | Fencing, quorum. Same as above — extremely complex. |
| 20 | Subscription management | Low | Medium | ★☆☆☆☆ | Only needed with market data. Out of scope without it. |
| 21 | Replica analytics (6 items) | Low | Medium | ★☆☆☆☆ | Throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL accounting. Nice demos, but buyers build their own analytics. |
| 22 | Protocol optims investigation | Low | Unknown | ★☆☆☆☆ | Research, not a feature. No commercial value until proven. |
