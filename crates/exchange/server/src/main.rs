/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuning, applied at allocator init via the well-known
/// `malloc_conf` symbol. Set for tail-latency stability:
///
/// - `background_thread:true` — spawn a dedicated thread to do page
///   purging asynchronously instead of synchronously on the allocating
///   thread. Default jemalloc does the purge work on whatever thread
///   happens to free memory, which on the matching/journal hot path
///   shows up as occasional multi-millisecond stalls in `process_event`.
/// - `dirty_decay_ms:53000` / `muzzy_decay_ms:57000` — hold dirty/muzzy
///   pages for ~53/57 s (vs the 10 s default) before reclaiming. Trades
///   marginally higher steady-state RSS for fewer purge events; with
///   the background thread this also bounds how often that thread runs.
///   Values are deliberately odd so purge-induced latency spikes are
///   immediately attributable to jemalloc rather than blending into
///   60 s monitoring/heartbeat boundaries.
///
/// Symbol details, because getting any of them wrong silently disables
/// the tuning (jemalloc falls back to its defaults without complaint):
///
/// - tikv-jemalloc-sys builds jemalloc with `--with-jemalloc-prefix=_rjem_`
///   on Linux, so the variable it reads is `_rjem_malloc_conf`, not
///   `malloc_conf`.
/// - jemalloc declares it as `const char *malloc_conf` — a thin pointer,
///   hence the `*const c_char` newtype (a `&[u8]` would be a fat pointer).
/// - `#[used]` keeps the static in the binary even though nothing in Rust
///   references it; jemalloc's own definition is weak, so ours wins.
///
/// [`log_jemalloc_config`] reads the effective values back through
/// `mallctl` at startup so a regression here shows up in the log.
#[repr(transparent)]
struct MallocConf(*const std::ffi::c_char);
// SAFETY: the pointer targets an immutable, 'static, NUL-terminated
// literal; jemalloc only ever reads through it.
unsafe impl Sync for MallocConf {}
#[used]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: MallocConf =
    MallocConf(c"background_thread:true,dirty_decay_ms:53000,muzzy_decay_ms:57000".as_ptr());

/// Expected values, kept next to the string above so the read-back check
/// can't drift from it.
const EXPECTED_BACKGROUND_THREAD: bool = true;
const EXPECTED_DIRTY_DECAY_MS: isize = 53_000;
const EXPECTED_MUZZY_DECAY_MS: isize = 57_000;

/// Read one jemalloc option via `mallctl` into a plain-old-data `T`.
fn mallctl_read<T: Copy>(name: &std::ffi::CStr) -> Result<T, i32> {
    let mut value = std::mem::MaybeUninit::<T>::uninit();
    let mut len = std::mem::size_of::<T>();
    // SAFETY: `name` is NUL-terminated; `value` has room for exactly
    // `len` bytes and jemalloc writes at most `len` bytes into it; no
    // new value is being written (null `newp`, zero `newlen`).
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 {
        // SAFETY: rc == 0 means jemalloc filled the value.
        Ok(unsafe { value.assume_init() })
    } else {
        Err(rc)
    }
}

/// Log the allocator options actually in effect and warn if they don't
/// match `MALLOC_CONF` — i.e. the tuning silently failed to apply.
fn log_jemalloc_config() {
    // jemalloc's mallctl types: `opt.background_thread` is `bool`,
    // the decay options are `ssize_t` (isize here; -1 means "never").
    let background_thread = mallctl_read::<bool>(c"opt.background_thread");
    let dirty_decay_ms = mallctl_read::<isize>(c"opt.dirty_decay_ms");
    let muzzy_decay_ms = mallctl_read::<isize>(c"opt.muzzy_decay_ms");
    match (background_thread, dirty_decay_ms, muzzy_decay_ms) {
        (Ok(bt), Ok(dirty), Ok(muzzy)) => {
            if bt == EXPECTED_BACKGROUND_THREAD
                && dirty == EXPECTED_DIRTY_DECAY_MS
                && muzzy == EXPECTED_MUZZY_DECAY_MS
            {
                tracing::info!(
                    background_thread = bt,
                    dirty_decay_ms = dirty,
                    muzzy_decay_ms = muzzy,
                    "jemalloc tuning applied"
                );
            } else {
                tracing::warn!(
                    background_thread = bt,
                    dirty_decay_ms = dirty,
                    muzzy_decay_ms = muzzy,
                    "jemalloc tuning NOT applied (malloc_conf ignored?) — \
                     purge stalls may land on the hot path"
                );
            }
        }
        (bt, dirty, muzzy) => tracing::warn!(
            ?bt,
            ?dirty,
            ?muzzy,
            "could not read jemalloc options via mallctl"
        ),
    }
}

use clap::Parser;
use melin_server::app_factory::{Factory, FactoryConfig};
use melin_server::event_publisher;
use melin_server::request_decoder::RequestDecoder;
use melin_server::response_encoder::ResponseEncoder;
use melin_server_runtime::server::{self, ServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    log_jemalloc_config();

    let config = ServerConfig::parse();

    let factory = Factory::new(FactoryConfig {
        accounts: config.accounts,
        instruments: config.instruments,
        max_orders_per_account: config.max_orders_per_account,
        max_orders_per_second: config.max_orders_per_second,
        max_orders_burst: config.max_orders_burst,
    });

    server::run(
        config,
        factory,
        RequestDecoder,
        ResponseEncoder,
        Some(event_publisher::run),
    )
}
