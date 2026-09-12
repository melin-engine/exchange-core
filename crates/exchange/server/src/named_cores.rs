//! `thread=core` core lists for the benches, in the shape of the
//! server's `--cores`.
//!
//! One syntax for every binary in the workspace: a layout written for the
//! server reads the same in a bench, and a bench thread can be added or
//! retired without renumbering every invocation — the reason the server
//! gave up positional lists. The benches keep their own thread sets and
//! wait policies; only the parsing is shared, so the two cannot drift
//! apart on what an entry looks like.

/// Parse `spec` as `name=core` entries, in any order, for exactly the
/// threads in `names`. Returns one core per name, in `names` order. `0`
/// leaves a thread unpinned and `none` leaves every thread unpinned. A
/// positional list, an unknown, repeated or missing name, or an unparsable
/// core is an error that names `flag`.
pub fn parse_named_cores(flag: &str, spec: &str, names: &[&str]) -> Result<Vec<usize>, String> {
    if spec.trim() == "none" {
        return Ok(vec![0; names.len()]);
    }
    // `Vec`, not an array: `names` is a slice whose length only the caller
    // knows. One slot per name, filled as entries arrive, so a repeated or
    // missing name is a slot seen twice or never.
    let mut cores: Vec<Option<usize>> = vec![None; names.len()];
    for entry in spec.split(',') {
        let entry = entry.trim();
        let Some((name, core)) = entry.split_once('=') else {
            if entry.parse::<usize>().is_ok() {
                return Err(format!(
                    "{flag} takes named entries in any order ({}), not a positional list; \
                     `none` unpins every thread",
                    example(names)
                ));
            }
            return Err(format!(
                "{flag}: invalid entry `{entry}`, expected `thread=core`"
            ));
        };
        let Some(slot) = names.iter().position(|n| *n == name) else {
            return Err(format!(
                "{flag}: unknown thread `{name}`; the threads are {}",
                names.join(", ")
            ));
        };
        if cores[slot].is_some() {
            return Err(format!("{flag}: `{name}` is named twice"));
        }
        let core = core
            .parse::<usize>()
            .map_err(|_| format!("{flag}: {name}: invalid core ID `{core}`"))?;
        cores[slot] = Some(core);
    }
    let missing: Vec<&str> = names
        .iter()
        .zip(&cores)
        .filter(|(_, core)| core.is_none())
        .map(|(name, _)| *name)
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "{flag}: no core for {}; every thread needs an entry (`0` leaves it unpinned)",
            missing.join(", ")
        ));
    }
    // Every slot is `Some`: a `None` was rejected just above.
    Ok(cores.into_iter().flatten().collect())
}

/// `a=1,b=2,...` over `names`, for the positional-list error.
fn example(names: &[&str]) -> String {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| format!("{name}={}", i + 1))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREADS: [&str; 3] = ["alpha", "beta", "gamma"];

    #[test]
    fn entries_come_back_in_name_order_whatever_their_order_in_the_spec() {
        let cores = parse_named_cores("--x", "gamma=3, alpha=1,beta=0", &THREADS).unwrap();
        assert_eq!(cores, vec![1, 0, 3]);
    }

    #[test]
    fn none_unpins_every_thread() {
        assert_eq!(
            parse_named_cores("--x", "none", &THREADS).unwrap(),
            vec![0, 0, 0]
        );
    }

    #[test]
    fn a_positional_list_is_refused_with_the_named_form_spelled_out() {
        let err = parse_named_cores("--x", "1,2,3", &THREADS).unwrap_err();
        assert!(err.contains("--x"), "{err}");
        assert!(err.contains("positional"), "{err}");
        assert!(err.contains("alpha=1,beta=2,gamma=3"), "{err}");
        assert!(err.contains("`none`"), "{err}");
    }

    #[test]
    fn unknown_repeated_missing_and_malformed_entries_are_refused() {
        let err = parse_named_cores("--x", "alpha=1,delta=2,gamma=3", &THREADS).unwrap_err();
        assert!(err.contains("unknown thread `delta`"), "{err}");
        assert!(err.contains("alpha, beta, gamma"), "{err}");

        let err = parse_named_cores("--x", "alpha=1,alpha=2,gamma=3", &THREADS).unwrap_err();
        assert!(err.contains("`alpha` is named twice"), "{err}");

        let err = parse_named_cores("--x", "alpha=1", &THREADS).unwrap_err();
        assert!(err.contains("no core for beta, gamma"), "{err}");

        let err = parse_named_cores("--x", "alpha=1,beta=2y,gamma=3", &THREADS).unwrap_err();
        assert!(err.contains("beta: invalid core ID `2y`"), "{err}");

        let err = parse_named_cores("--x", "alpha=1,beta,gamma=3", &THREADS).unwrap_err();
        assert!(err.contains("invalid entry `beta`"), "{err}");
    }
}
