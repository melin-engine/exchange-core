# Performance Profile

Engine-mode profiling at [`56e3f10`](../../commit/56e3f10) on Apple M1 (Asahi Linux), 20M orders, `perf record -D 3000` to skip warmup.

## Raw Profile

The benchmark loop spends ~58% of total time in two `mrs cntvct_el0` (ARM counter) instructions — measurement overhead, not engine work. The table below shows all functions above 0.3% of total samples.

| % total | % excl. measurement | Function | Category |
|---------|---------------------|----------|----------|
| 58.39% | — | `trading_bench::main` (97.5% is cntvct reads) | Bench harness |
| 8.59% | **20.6%** | `AccountManager::process_reports` | Engine |
| 7.18% | 17.2% | `OrderFlowGenerator::next_event` | Bench generator |
| 3.62% | 8.7% | `pow@@GLIBC` | Bench generator (price distribution) |
| 3.45% | **8.3%** | `Exchange::execute` | Engine |
| 1.87% | **4.5%** | `hashbrown::HashMap::insert` (site 1) | Engine (FxHashMap) |
| 1.64% | **3.9%** | `OrderBook::cancel` | Engine |
| 1.31% | **3.1%** | `OrderBook::execute` | Engine |
| 1.22% | 2.9% | `OrderFlowGenerator::pick_price` | Bench generator |
| 1.10% | **2.6%** | `AccountManager::try_adjust_reservation` | Engine |
| 0.86% | **2.1%** | `hashbrown::HashMap::insert` (site 2) | Engine (FxHashMap) |
| 0.86% | **2.1%** | `alloc::raw_vec::finish_grow` | Engine (VecDeque realloc) |
| 0.85% | **2.0%** | `Exchange::cancel_replace` | Engine |
| 0.78% | **1.9%** | `OrderBook::execute_limit` | Engine |
| 0.72% | 1.7% | `OrderFlowGenerator::pick_size` | Bench generator |
| 0.72% | **1.7%** | `BookSide::add` | Engine |
| 0.69% | **1.7%** | `OrderBook::replace_order` | Engine |
| 0.59% | **1.4%** | `OrderBook::match_against` | Engine |
| 0.58% | 1.4% | `hdrhistogram::record_n_inner` | Bench harness |
| 0.57% | **1.4%** | `Exchange::cancel` | Engine |
| 0.54% | 1.3% | `_rjem_malloc` (jemalloc) | Allocator |
| 0.41% | 1.0% | `_rjem_sdallocx` (jemalloc) | Allocator |
| 0.41% | **1.0%** | `alloc::raw_vec::grow_one` | Engine (VecDeque realloc) |
| 0.38% | **0.9%** | `VecDeque::wrap_copy` | Engine |
| 0.33% | **0.8%** | `u128_div_rem` (compiler_builtins) | Engine (fee calc) |

## Engine-Only Breakdown

Excluding bench harness (~58%), generator (~13%), and allocator (~1%), the engine accounts for ~28% of total time. Normalized to engine-only:

| Engine % | Function | Notes |
|----------|----------|-------|
| **30.7%** | `process_reports` | Loops over execution reports, FxHashMap lookups, Vec::contains |
| **12.3%** | `Exchange::execute` | Submit dispatch, validation, reserve, post-matching cleanup |
| **6.6%** | `hashbrown::insert` (combined) | FxHashMap insert into order_info + order_index |
| **5.9%** | `OrderBook::cancel` | Index lookup + sorted Vec removal |
| **4.7%** | `OrderBook::execute` | Matching entry point |
| **3.9%** | `try_adjust_reservation` | Cancel-replace reservation adjustment |
| **3.1%** | `alloc` (combined grow + finish_grow) | VecDeque reallocation at price levels |
| **2.9%** | `Exchange::cancel_replace` | Amend path |
| **2.8%** | `OrderBook::execute_limit` | Limit order processing |
| **2.4%** | `BookSide::add` | Binary search + insert into sorted Vec |
| **2.4%** | `OrderBook::replace_order` | Book-level amend |
| **2.0%** | `match_against` | Price level iteration + fill loop |
| **2.0%** | `Exchange::cancel` | Cancel dispatch |
| **1.3%** | `VecDeque::wrap_copy` | Internal VecDeque bookkeeping |
| **1.2%** | `u128_div_rem` | Fee calculation (software-emulated u128 division on ARM) |

## Key Observations

1. **process_reports is 30.7% of engine time** — the single biggest target. It loops over ExecutionReport variants and performs FxHashMap lookups for each fill (maker + taker). The `Vec::contains` check for double-free prevention adds O(n) scanning.

2. **FxHashMap insert is 6.6%** — two insert sites for order_info (Exchange) and order_index (OrderBook) on every submit. A monotonic sequence ID indexing flat Vecs would eliminate these.

3. **VecDeque reallocation is 3.1%** — price level queues grow dynamically. Pre-allocating with a small initial capacity (e.g., 8 orders) would avoid most reallocations.

4. **u128_div_rem is 1.2%** — fee calculation uses `cost * fee_bps / 10_000` with u128. On ARM, u128 division is emulated in software (~50 cycles). Could use multiply-shift: `cost * fee_bps >> 17` is close to `/10_000` (off by 0.0078%), or precompute a reciprocal.

5. **Measurement overhead dominates** — the ARM cntvct_el0 counter read is ~40-50ns per call (two per order = ~80-100ns). On x86 Cherry servers with rdtsc (~4ns), the engine fraction will be much higher.

## Platform Note

This profile is from Apple M1 (Asahi). The Cherry AMD Ryzen 9950X production servers will show different ratios:
- `rdtsc` is ~10x faster than `cntvct_el0` — measurement overhead drops from ~58% to ~10%
- x86 has native u128 division (`div r64`) — the `u128_div_rem` cost disappears
- Different cache hierarchy and branch predictor behavior
