//! The seam between the harness and whatever it is benchmarking.
//!
//! The harness owns timing, pacing, phases, and the event loop. It knows
//! nothing about what a request means — only that a workload can produce
//! request frames and can tell it which response frames complete one.
//! Everything application-specific (the codec, the order-flow model, the
//! reject taxonomy) lives on the other side of this trait.
//!
//! One [`Workload`] instance per connection; the harness never shares one
//! across threads.

/// Per-connection request generation and response classification.
///
/// # Why decoding, completion, and tallying are three separate calls
///
/// The obvious shape — one `on_response(frame) -> Verdict` that decodes,
/// classifies, and counts — would put the tally's cost inside the measured
/// interval. The harness captures its receive timestamp between
/// [`completes_request`](Workload::completes_request) and
/// [`record`](Workload::record) precisely so a latency sample reflects the
/// wire roundtrip and not the benchmark's own bookkeeping. Splitting the
/// calls keeps that ordering expressible without decoding twice.
pub trait Workload: Send {
    /// Decoded form of one response frame. Carried from
    /// [`decode`](Workload::decode) to the classification and tally calls
    /// so a frame is parsed exactly once.
    type Response;

    /// Per-connection tallies, merged across connections and threads at
    /// end of run.
    type Outcomes: Outcomes;

    /// Append exactly one length-prefixed request frame to `out`.
    ///
    /// `out` accumulates a batch of frames and is drained by the harness
    /// after the write completes, so implementations must append rather
    /// than overwrite. Called on the hot path: no allocation, no I/O.
    fn next_frame(&mut self, out: &mut Vec<u8>);

    /// Decode one response frame body — the length prefix is already
    /// stripped by the harness, which owns the framing.
    ///
    /// Implementations may panic on an undecodable frame: a benchmark
    /// against a server speaking a different protocol has no meaningful
    /// result to report, so failing loudly beats silently skewing one.
    fn decode(&self, frame: &[u8]) -> Self::Response;

    /// Whether this response completes one in-flight request, meaning the
    /// harness should pop a send timestamp and record a latency sample.
    ///
    /// Must be true exactly once per frame produced by
    /// [`next_frame`](Workload::next_frame). Returning it more often
    /// underflows the in-flight queue (the harness panics on a completion
    /// with no matching send); less often leaks queue slots and stalls the
    /// send window.
    fn completes_request(&self, response: &Self::Response) -> bool;

    /// Fold one response into this connection's tallies. Called for every
    /// frame in every phase — warmup, measured, and cooldown — so the
    /// counts describe the whole run, unlike the latency histogram.
    fn record(&mut self, response: &Self::Response);

    /// This connection's tallies, read once after the bench threads join.
    fn outcomes(&self) -> &Self::Outcomes;
}

/// Tallies a [`Workload`] accumulates per connection and the harness folds
/// together at end of run.
pub trait Outcomes: Default + Send {
    /// Fold `other` into `self`. Must be associative and commutative — the
    /// harness merges per-connection tallies in thread-completion order,
    /// which is not deterministic.
    fn merge(&mut self, other: &Self);

    /// Print the outcome section of the console summary, or nothing if
    /// this workload has nothing to add beyond throughput and latency.
    ///
    /// Called after the latency histogram and before the health summary.
    /// Implementations own their own blank lines and indentation; the rest
    /// of the report indents section bodies by four spaces under a
    /// two-space heading.
    ///
    /// This is where a run that produced numbers but not *useful* numbers
    /// gets caught — a flood of rejections looks identical to clean
    /// traffic in the latency histogram.
    fn render_console(&self);

    /// Render these tallies as the JSON `outcomes` object, braces
    /// included. Return `{}` when there is nothing to report: the key is
    /// always present so consumers can rely on its shape.
    fn render_json(&self) -> String;
}
