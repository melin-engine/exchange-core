#!/usr/bin/env bash
set -euo pipefail

# Cut a release: bump the workspace version, promote the changelog, stamp the
# BSL Change Date into LICENSE, commit on a release branch, tag, push, and
# publish.
#
# Usage:
#   scripts/release.sh 0.16.0                    # rehearsal (see below)
#   scripts/release.sh 0.16.0 --execute          # for real: pushes and publishes
#   scripts/release.sh 0.15.1 --from release/0.15.x   # patch a maintenance line
#   scripts/release.sh 0.16.0 --change-date 2030-01-01
#   scripts/release.sh 0.16.0 --execute --yes    # skip the confirmation prompt
#   scripts/release.sh 0.16.0 --keep             # rehearsal, keep what it built
#
# Without `--execute` this is a full rehearsal rather than a preview: it makes
# the branch, the edits, the commit (so the pre-commit hook runs the real
# release gate — fmt, clippy, the DPDK and required-features builds, the suite)
# and the tag, runs `publish.sh` in its dry-run mode, and then puts the
# repository back exactly as it found it. Nothing leaves the machine. `--keep`
# skips the restore when you want to inspect the result; a rehearsal that
# *fails* also keeps everything, so there is something left to debug, and
# prints how to clean up. The branch and up-to-date checks are warnings there
# rather than errors, since a rehearsal has no remote or registry to protect.
#
# What it deliberately does not do:
#
#   * Refresh third-party dependencies. Run `cargo update` as its own
#     `chore(deps)` commit on the integration branch first, so the release
#     commit stays a pure version bump and the refreshed lockfile has been
#     through the gate before anything is tagged.
#   * Merge. The tag is cut on `release/X.Y.Z` and the human merges it with
#     `--no-ff`, as every release so far has been; the tagged commit stays
#     reachable from the integration branch through the merge. Do it
#     promptly: the up-to-date check below only proves that branch was current
#     when the release started.
#   * Write the changelog for you. It promotes what is already under
#     `## [Unreleased]` into the new version's entry and opens a fresh empty
#     one; it refuses to release if that section is empty. Verifying that a
#     published version *has* an entry stays in `publish.sh`, so a publish run
#     started by hand is guarded too.

# --- Arguments ---------------------------------------------------------------

NEW_VERSION=""
EXECUTE=0
KEEP=0
ASSUME_YES=0
CHANGE_DATE=""
FROM_BRANCH="main"

usage() {
    echo "usage: scripts/release.sh <version> [--execute] [--from BRANCH]" >&2
    echo "                          [--change-date YYYY-MM-DD] [--yes] [--keep]" >&2
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute) EXECUTE=1; shift ;;
        --keep) KEEP=1; shift ;;
        --yes|-y) ASSUME_YES=1; shift ;;
        --from) FROM_BRANCH="${2:-}"; [[ -n "$FROM_BRANCH" ]] || usage; shift 2 ;;
        --change-date) CHANGE_DATE="${2:-}"; [[ -n "$CHANGE_DATE" ]] || usage; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "error: unknown option '$1'" >&2; usage ;;
        *)
            [[ -z "$NEW_VERSION" ]] || { echo "error: version given twice" >&2; usage; }
            NEW_VERSION="$1"; shift ;;
    esac
done

[[ -n "$NEW_VERSION" ]] || usage

# Plain X.Y.Z only. Pre-release suffixes are rejected rather than half-handled:
# `sort -V` below orders `0.16.0-rc.1` *after* `0.16.0`, which is backwards
# under semver, so accepting them would mean an ordering check that lies.
if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: '$NEW_VERSION' is not a plain X.Y.Z version" >&2
    exit 1
fi

# Branch and tag shapes match the releases cut by hand before this script:
# `release/0.15.0`, tagged `v0.15.0`.
BRANCH="release/$NEW_VERSION"
TAG="v$NEW_VERSION"

# The BSL Change Date. Default is four years out, which is what the licence
# text falls back to anyway ("the fourth anniversary of the first publicly
# available distribution") — so the default changes nothing legally and only
# removes the placeholder. Override to convert sooner; converting later than
# the fallback is not possible.
if [[ -z "$CHANGE_DATE" ]]; then
    CHANGE_DATE=$(date -u -d "+4 years" +%F) || {
        echo "error: GNU date required (for '-d +4 years'); pass --change-date instead" >&2
        exit 1
    }
fi
if [[ ! "$CHANGE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --change-date must be YYYY-MM-DD, got '$CHANGE_DATE'" >&2
    exit 1
fi

# --- Failure guidance --------------------------------------------------------

# Tracked so the exit trap can say what actually happened. A release that dies
# after the push is a very different situation from one that dies before it,
# and the difference is not recoverable from the exit code.
DID_BRANCH=0
DID_COMMIT=0
DID_TAG=0
DID_PUSH=0
DID_PUBLISH_START=0
ORIGINAL_BRANCH=""

cleanup_hint() {
    echo "    Undo the local work with:"
    (( DID_TAG )) && echo "      git tag -d $TAG"
    # Before the commit, the bump and the stamped licence are uncommitted
    # edits; a plain checkout would carry them back onto the original branch.
    (( DID_BRANCH && ! DID_COMMIT )) && echo "      git checkout -- ."
    echo "      git checkout ${ORIGINAL_BRANCH:-main}"
    (( DID_BRANCH )) && echo "      git branch -D $BRANCH"
    return 0
}

on_exit() {
    local status=$?
    (( status == 0 )) && return 0
    echo >&2
    echo "==> release.sh failed (exit $status)." >&2
    if (( DID_PUBLISH_START )); then
        echo "    Publishing had already begun — some crates may be live on crates.io." >&2
        echo "    crates.io publishes are permanent: do NOT retry under a new version." >&2
        echo "    Resume with 'scripts/publish.sh --execute'; it skips crates already published." >&2
    elif (( DID_PUSH )); then
        echo "    $BRANCH and $TAG are already on origin — left in place." >&2
        echo "    Resume with 'scripts/publish.sh --execute' once the failure is understood." >&2
    else
        echo "    Nothing left this machine." >&2
        cleanup_hint >&2
    fi
} >&2
trap on_exit EXIT

step() { echo; echo "==> $*"; }

# `grep -c` exits 1 on no match, which `set -e` would turn into an abort — but
# "zero matches" is an answer these checks need, not a failure.
count() { grep -c -- "$1" "$2" || true; }

# The checks that exist to protect the remote and the registry. A rehearsal
# pushes nothing and publishes nothing, so there they have nothing to protect
# and become warnings — which is also what lets this script rehearse a change
# to itself, from the branch carrying that change.
guard() {
    if (( EXECUTE )); then
        echo "error: $*" >&2
        exit 1
    fi
    echo "warning: $* — ignored in rehearsal" >&2
}

# --- Preconditions -----------------------------------------------------------

cd "$(git rev-parse --show-toplevel)"

if (( EXECUTE )); then
    echo "==> LIVE release mode — this will push and publish"
else
    echo "==> Rehearsal mode (pass --execute to release for real)"
fi

step "Checking the tools"
for tool in jq sha256sum; do
    command -v "$tool" >/dev/null || { echo "error: $tool is required" >&2; exit 1; }
done

step "Checking the working tree"
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: working tree has uncommitted changes; commit or stash first" >&2
    exit 1
fi
if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    echo "warning: untracked files present; they will not be part of the release" >&2
fi

# A release is cut from an integration branch that is already on origin, at
# exactly its remote tip. Both halves matter and for different reasons: the
# tag has to stay reachable from the line it claims to belong to, and what
# gets published has to be code that branch actually carries — crates.io is
# immutable, so a version cut from somewhere else cannot be taken back.
#
# `main` by default, `--from` for a maintenance line (the shape a patch
# release off an older version takes). Requiring it to be *named* rather than
# merely allowed is the point: it makes releasing from a feature branch
# something you do on purpose, not by forgetting to check out main.
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$ORIGINAL_BRANCH" != "$FROM_BRANCH" ]]; then
    guard "on '$ORIGINAL_BRANCH' but releasing from '$FROM_BRANCH'; check it out, or name it with --from"
fi

step "Checking $ORIGINAL_BRANCH is current"
if git fetch --quiet origin "$ORIGINAL_BRANCH" 2>/dev/null; then
    if [[ "$(git rev-parse HEAD)" != "$(git rev-parse FETCH_HEAD)" ]]; then
        guard "local $ORIGINAL_BRANCH differs from origin/$ORIGINAL_BRANCH; pull (or push) first"
    fi
else
    guard "$ORIGINAL_BRANCH is not on origin; push it first"
fi

step "Checking $BRANCH and $TAG are free"
if git rev-parse --verify --quiet "refs/heads/$BRANCH" >/dev/null; then
    echo "error: branch $BRANCH already exists locally" >&2
    exit 1
fi
if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then
    echo "error: tag $TAG already exists locally" >&2
    exit 1
fi
if git ls-remote --exit-code --heads origin "refs/heads/$BRANCH" >/dev/null 2>&1; then
    echo "error: branch $BRANCH already exists on origin" >&2
    exit 1
fi
if git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
    echo "error: tag $TAG already exists on origin — $NEW_VERSION is already released" >&2
    exit 1
fi

# The commit below runs the pre-commit hook as the release gate — but the hook
# is opt-in (`git config core.hooksPath .githooks`), and on a machine that
# never ran that, `git commit` checks nothing. In live mode what follows is a
# push and a publish with no CI in between, so this is a hard error in both
# modes: a rehearsal that skips the gate proves nothing.
step "Checking the pre-commit hook is installed"
if [[ "$(git config --get core.hooksPath || true)" != ".githooks" ]]; then
    echo "error: the pre-commit hook is not installed; run 'git config core.hooksPath .githooks'" >&2
    exit 1
fi

step "Checking the lockfile resolves"
cargo metadata --locked --format-version 1 >/dev/null

# The single source of truth is `[workspace.package] version` in the root
# manifest; every member inherits it. Read it from there rather than from
# cargo metadata, because that is the line the bump has to rewrite.
OLD_VERSION=$(grep -m1 '^version = "' Cargo.toml || true)
OLD_VERSION=${OLD_VERSION#version = \"}
OLD_VERSION=${OLD_VERSION%\"}
if [[ -z "$OLD_VERSION" ]]; then
    echo "error: could not read the current version from Cargo.toml" >&2
    exit 1
fi

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
    echo "error: workspace is already at $NEW_VERSION" >&2
    exit 1
fi
if ! printf '%s\n%s\n' "$OLD_VERSION" "$NEW_VERSION" | sort -V -C; then
    echo "error: $NEW_VERSION is not greater than the current $OLD_VERSION" >&2
    exit 1
fi

# `publish.sh` enforces that the version being published has an entry — it has
# to, since it also runs by hand. Checked here as well, because there it lands
# *after* the branch and tag have been pushed, and a tag on origin for a
# version that cannot be published is not something to discover at that
# point. What is checked differs: the entry does not exist yet at this stage,
# so what must hold is that there is something to promote into it.
step "Checking CHANGELOG.md is ready for $NEW_VERSION"
if [[ ! -f CHANGELOG.md ]]; then
    echo "error: CHANGELOG.md is missing; every released version needs an entry" >&2
    exit 1
fi
if (( $(count '^## \[Unreleased\]$' CHANGELOG.md) != 1 )); then
    echo "error: CHANGELOG.md needs exactly one '## [Unreleased]' heading to promote" >&2
    exit 1
fi
if (( $(count "^## \[${NEW_VERSION//./\\.}\]" CHANGELOG.md) != 0 )); then
    echo "error: CHANGELOG.md already has a '## [$NEW_VERSION]' entry" >&2
    exit 1
fi
# A release whose changelog entry says nothing is worse than no changelog: it
# looks maintained. `### ` rather than any text, because the link definitions
# at the foot of the file would otherwise read as content when Unreleased is
# the only section.
if ! sed -n '/^## \[Unreleased\]$/,/^## \[/p' CHANGELOG.md | grep -q '^### '; then
    echo "error: the '## [Unreleased]' section is empty; nothing to release" >&2
    exit 1
fi

echo
echo "    version:     $OLD_VERSION -> $NEW_VERSION"
echo "    branch:      $BRANCH"
echo "    tag:         $TAG"
echo "    Change Date: $CHANGE_DATE"

if (( EXECUTE )) && (( ! ASSUME_YES )); then
    if [[ ! -t 0 ]]; then
        echo "error: --execute needs a terminal to confirm on; pass --yes" >&2
        exit 1
    fi
    echo
    read -r -p "Push $TAG and publish $NEW_VERSION to crates.io? Publishes are permanent. [y/N] " reply
    if [[ "$reply" != "y" && "$reply" != "Y" ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# --- Branch ------------------------------------------------------------------

step "Creating $BRANCH"
git checkout -q -b "$BRANCH"
DID_BRANCH=1

# --- Version bump ------------------------------------------------------------

# The root manifest carries the version in several places — the inherited
# `[workspace.package]` one plus a pin per intra-workspace dependency — and
# they must move in lockstep: a dependent resolving against a version its
# provider no longer has is a broken publish, discovered halfway through.
#
# Intra-workspace pins are the `melin-ec*` ones. The sequencer's crates share
# the `melin-` prefix but are crates.io dependencies on their own version
# line, so matching bare `melin-` would drag them into the bump.
#
# The substitution is counted at every step rather than trusted. Anything
# unexpected aborts instead of being silently half-applied, which is the whole
# risk of rewriting a manifest with a regex.
step "Bumping the workspace version"

# Only `.` is a regex metacharacter in a version string the X.Y.Z check above
# has already proven to be digits and dots.
OLD_RE=${OLD_VERSION//./\\.}
NEW_RE=${NEW_VERSION//./\\.}

WS_AT_OLD='^version = "'"$OLD_RE"'"$'
DEP_AT_OLD='^melin-ec[A-Za-z0-9_-]* = { path = "[^"]*", version = "'"$OLD_RE"'"'
# Deliberately looser than the substitution shape, so a pin written differently
# is caught here rather than quietly skipped by the sed below.
DEP_ANY='^melin-ec[A-Za-z0-9_-]* = .*version = "'

STALE=$(grep -n -- "$DEP_ANY" Cargo.toml | grep -v -- "version = \"$OLD_RE\"" || true)
if [[ -n "$STALE" ]]; then
    echo "error: intra-workspace pins are not all at $OLD_VERSION; fix them first:" >&2
    echo "$STALE" >&2
    exit 1
fi

WS_BEFORE=$(count "$WS_AT_OLD" Cargo.toml)
DEP_BEFORE=$(count "$DEP_AT_OLD" Cargo.toml)
DEP_TOTAL=$(count "$DEP_ANY" Cargo.toml)

if (( WS_BEFORE != 1 )); then
    echo "error: expected 1 [workspace.package] version line at $OLD_VERSION, found $WS_BEFORE" >&2
    exit 1
fi
if (( DEP_BEFORE == 0 )); then
    echo "error: found no intra-workspace pins to bump; has the manifest shape changed?" >&2
    exit 1
fi
if (( DEP_BEFORE != DEP_TOTAL )); then
    echo "error: only $DEP_BEFORE of $DEP_TOTAL intra-workspace pins match the rewrite shape" >&2
    exit 1
fi

sed -i \
    -e "s/^version = \"$OLD_RE\"\$/version = \"$NEW_VERSION\"/" \
    -e "s/^\\(melin-ec[A-Za-z0-9_-]* = { path = \"[^\"]*\", version = \\)\"$OLD_RE\"/\\1\"$NEW_VERSION\"/" \
    Cargo.toml

# Scoped to those two shapes on purpose: a third-party or sequencer dependency
# that happens to sit at the same version number is not our problem and must
# not block a release.
LEFT=$(( $(count "$WS_AT_OLD" Cargo.toml) + $(count "$DEP_AT_OLD" Cargo.toml) ))
if (( LEFT != 0 )); then
    echo "error: $LEFT version string(s) still at $OLD_VERSION after the bump" >&2
    exit 1
fi

WS_AFTER=$(count '^version = "'"$NEW_RE"'"$' Cargo.toml)
DEP_AFTER=$(count '^melin-ec[A-Za-z0-9_-]* = { path = "[^"]*", version = "'"$NEW_RE"'"' Cargo.toml)
if (( WS_AFTER != WS_BEFORE || DEP_AFTER != DEP_BEFORE )); then
    echo "error: rewrote $WS_AFTER+$DEP_AFTER version strings, expected $WS_BEFORE+$DEP_BEFORE" >&2
    exit 1
fi
echo "    rewrote $(( WS_AFTER + DEP_AFTER )) version strings in Cargo.toml"

# --- Changelog ---------------------------------------------------------------

# What accumulated under `## [Unreleased]` becomes this version's entry, and a
# fresh empty `## [Unreleased]` takes its place. Done here rather than by hand
# beforehand because it belongs in the release commit, next to the version bump
# and the Change Date — and because a script that refuses to run on a dirty
# tree cannot ask you to edit a tracked file first.
step "Promoting the changelog's Unreleased section"

RELEASE_DATE=$(date -u +%F)

# Renaming and inserting are one substitution: the old heading becomes the new
# pair. GNU sed only, for `\n` in the replacement — as with `date -d` above.
sed -i "s|^## \[Unreleased\]\$|## [Unreleased]\n\n## [$NEW_VERSION] - $RELEASE_DATE|" CHANGELOG.md

# Keep a Changelog defines its versions as link references at the foot of the
# file. The base URL is taken from the line already there rather than written
# into this script, so a repository move needs no edit here.
UNRELEASED_LINK=$(grep -m1 '^\[Unreleased\]: ' CHANGELOG.md || true)
if [[ -n "$UNRELEASED_LINK" ]]; then
    LINK_BASE=${UNRELEASED_LINK#\[Unreleased\]: }
    LINK_BASE=${LINK_BASE%/compare/*}
    sed -i \
        -e "s|^\[Unreleased\]: .*\$|[Unreleased]: $LINK_BASE/compare/v$NEW_VERSION...HEAD|" \
        -e "/^\[Unreleased\]: /a [$NEW_VERSION]: $LINK_BASE/releases/tag/v$NEW_VERSION" \
        CHANGELOG.md
fi

if (( $(count '^## \[Unreleased\]$' CHANGELOG.md) != 1 )); then
    echo "error: promotion left $(count '^## \[Unreleased\]$' CHANGELOG.md) Unreleased headings" >&2
    exit 1
fi
if (( $(count "^## \[$NEW_RE\] - $RELEASE_DATE\$" CHANGELOG.md) != 1 )); then
    echo "error: promotion did not produce a '## [$NEW_VERSION] - $RELEASE_DATE' heading" >&2
    exit 1
fi
echo "    promoted Unreleased to $NEW_VERSION ($RELEASE_DATE)"

# `--workspace` restricts the update to workspace members, so a release cannot
# quietly drag in new third-party versions along with the bump.
step "Refreshing Cargo.lock"
cargo update --quiet --workspace
cargo metadata --locked --format-version 1 >/dev/null

# --- BSL Change Date ---------------------------------------------------------

# Every crate declares `license-file` pointing at the root LICENSE, and cargo
# copies that file into each package it builds — so stamping the root is
# stamping every published crate. That only holds while each crate really
# points there: a crate that grew its own licence file would ship with
# whatever date that copy carries, so the assumption is checked rather than
# trusted. `.publish` is absent (null) on a publishable crate and `[]` on one
# marked `publish = false`; the latter never ships and is not checked.
step "Stamping the BSL Change Date"

ROOT_LICENSE=$(realpath LICENSE)
# `license_file` is relative to the crate's own manifest directory. A crate with
# a plain `license` expression and no file prints `-` and is reported too:
# every published crate here must carry the BSL text.
MISMATCHED=$(
    cargo metadata --no-deps --format-version 1 | jq -r '
        .packages[]
        | select(.publish != [])
        | "\(.name)\t\(.manifest_path | rtrimstr("/Cargo.toml"))\t\(.license_file // "-")"' \
    | while IFS=$'\t' read -r name dir file; do
        if [[ "$file" == "-" ]] || [[ "$(realpath -m "$dir/$file")" != "$ROOT_LICENSE" ]]; then
            echo "    $name: ${file}"
        fi
    done
)
if [[ -n "$MISMATCHED" ]]; then
    echo "error: these crates do not take their licence from the root LICENSE:" >&2
    echo "$MISMATCHED" >&2
    exit 1
fi

# Anchored to the start of the line so the two places the licence *prose*
# mentions the Change Date are left alone.
DATE_BEFORE=$(count '^Change Date:[[:space:]]' LICENSE)
if (( DATE_BEFORE != 1 )); then
    echo "error: expected exactly 1 'Change Date:' line in LICENSE, found $DATE_BEFORE" >&2
    exit 1
fi

# The parameter block aligns its values at column 23; keep that.
sed -i "s/^Change Date:[[:space:]].*\$/Change Date:          $CHANGE_DATE/" LICENSE

if (( $(count "^Change Date:          $CHANGE_DATE\$" LICENSE) != 1 )); then
    echo "error: Change Date was not stamped into LICENSE" >&2
    exit 1
fi
echo "    stamped $CHANGE_DATE into LICENSE"

# --- Commit and tag ----------------------------------------------------------

# No --no-verify: the pre-commit hook is the release gate. It runs fmt, clippy,
# the DPDK and required-features builds, and the suite, and a release is
# exactly the commit that must not skip them.
step "Committing"
git add Cargo.toml Cargo.lock CHANGELOG.md LICENSE
git commit -q -m "chore(release): $NEW_VERSION

Bump the workspace to $NEW_VERSION, promote the changelog's Unreleased
section to $NEW_VERSION ($RELEASE_DATE), and set the BSL Change Date to
$CHANGE_DATE."
DID_COMMIT=1

step "Tagging $TAG"
git tag -a "$TAG" -m "Release $NEW_VERSION"
DID_TAG=1

# --- Push and publish --------------------------------------------------------

if (( EXECUTE )); then
    # --atomic so the branch and the tag land together: a tag on origin whose
    # commit is not there is worse than neither.
    step "Pushing $BRANCH and $TAG"
    git push --atomic origin "$BRANCH" "$TAG"
    DID_PUSH=1

    step "Publishing to crates.io"
    DID_PUBLISH_START=1
    scripts/publish.sh --execute
else
    step "Skipping push (rehearsal)"

    # The same script the live path runs, one mode over. Its dry run resolves
    # the `melin-ec* = "$NEW_VERSION"` pins against the other packages in the
    # same run rather than against the registry, which is what lets a version
    # that has never been released be rehearsed at all.
    step "Packaging every crate (publish dry run)"
    scripts/publish.sh
fi

# --- Done --------------------------------------------------------------------

echo
if (( EXECUTE )); then
    echo "==> Released $TAG."
    echo "    Merge it — the tag is on $BRANCH, not yet on $ORIGINAL_BRANCH:"
    echo "      git checkout $ORIGINAL_BRANCH && git merge --no-ff $BRANCH && git push origin $ORIGINAL_BRANCH"
elif (( KEEP )); then
    echo "==> Rehearsal complete; $BRANCH and $TAG kept as asked."
    cleanup_hint
else
    git checkout -q "$ORIGINAL_BRANCH"
    git branch -q -D "$BRANCH"
    git tag -d "$TAG" >/dev/null
    echo "==> Rehearsal complete; repository restored to $ORIGINAL_BRANCH."
    echo "    Re-run with --execute to release $TAG for real."
fi
