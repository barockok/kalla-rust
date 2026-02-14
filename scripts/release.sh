#!/usr/bin/env bash
#
# release.sh -- Release script for Kalla (Universal Reconciliation Engine)
#
# Usage:
#   ./scripts/release.sh <version>
#   ./scripts/release.sh --dry-run <version>
#
# Examples:
#   ./scripts/release.sh 0.2.0
#   ./scripts/release.sh --dry-run 0.2.0
#
# This script performs the following steps:
#   1. Validates the version argument (semver X.Y.Z)
#   2. Checks for a clean git working tree
#   3. Checks that the current branch is main
#   4. Runs the full test suite (cargo test --workspace)
#   5. Updates version in Cargo.toml (workspace) and kalla-web/package.json
#   6. Builds Docker images via docker compose
#   7. Creates a git commit tagged with the release version
#   8. Prints instructions for pushing to the remote
#
# Flags:
#   --dry-run   Show what would happen without making any changes
#

set -euo pipefail

# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { printf "${GREEN}[INFO]${RESET}  %s\n" "$*"; }
warn()    { printf "${YELLOW}[WARN]${RESET}  %s\n" "$*"; }
error()   { printf "${RED}[ERROR]${RESET} %s\n" "$*" >&2; }

# ---------------------------------------------------------------------------
# Cleanup / failure handler
# ---------------------------------------------------------------------------
on_failure() {
    error "Release process failed. No changes have been pushed to the remote."
    error "Review the output above to determine what went wrong."
    exit 1
}
trap on_failure ERR

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DRY_RUN=false
VERSION=""

for arg in "$@"; do
    case "$arg" in
        --dry-run)
            DRY_RUN=true
            ;;
        -h|--help)
            # Print the header comment block as usage text
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            if [[ -z "$VERSION" ]]; then
                VERSION="$arg"
            else
                error "Unexpected argument: $arg"
                error "Usage: $0 [--dry-run] <version>"
                exit 1
            fi
            ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    error "Missing required version argument."
    error "Usage: $0 [--dry-run] <version>"
    exit 1
fi

# ---------------------------------------------------------------------------
# Resolve the project root (parent of the scripts/ directory)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ---------------------------------------------------------------------------
# Step 1: Validate version format (strict semver: X.Y.Z)
# ---------------------------------------------------------------------------
info "Validating version format: $VERSION"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    error "Invalid version format: '$VERSION'"
    error "Expected semantic version in the form X.Y.Z (e.g. 0.2.0, 1.0.0)"
    exit 1
fi

info "Version format is valid."

# ---------------------------------------------------------------------------
# Step 2: Check for a clean git working tree
# ---------------------------------------------------------------------------
info "Checking git working tree..."

if [[ -n "$(git status --porcelain)" ]]; then
    error "Git working tree is not clean. Please commit or stash your changes first."
    error "Uncommitted changes:"
    git status --short >&2
    exit 1
fi

info "Working tree is clean."

# ---------------------------------------------------------------------------
# Step 3: Check current branch is main
# ---------------------------------------------------------------------------
info "Checking current branch..."

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    error "Releases must be created from the 'main' branch."
    error "Current branch: $CURRENT_BRANCH"
    exit 1
fi

info "On branch 'main'."

# ---------------------------------------------------------------------------
# Step 4: Check that the tag does not already exist
# ---------------------------------------------------------------------------
info "Checking that tag v$VERSION does not already exist..."

if git rev-parse "v$VERSION" >/dev/null 2>&1; then
    error "Tag v$VERSION already exists. Choose a different version or delete the existing tag."
    exit 1
fi

info "Tag v$VERSION is available."

# ---------------------------------------------------------------------------
# Step 5: Run tests
# ---------------------------------------------------------------------------
info "Running cargo test --workspace ..."

if [[ "$DRY_RUN" == true ]]; then
    warn "[dry-run] Would run: cargo test --workspace"
else
    cargo test --workspace
    info "All tests passed."
fi

# ---------------------------------------------------------------------------
# Step 6: Update version numbers
# ---------------------------------------------------------------------------
CARGO_TOML="$PROJECT_ROOT/Cargo.toml"
PACKAGE_JSON="$PROJECT_ROOT/kalla-web/package.json"

CURRENT_CARGO_VERSION="$(grep -m1 '^version' "$CARGO_TOML" | sed 's/.*"\(.*\)".*/\1/')"
CURRENT_PKG_VERSION="$(grep -m1 '"version"' "$PACKAGE_JSON" | sed 's/.*: *"\(.*\)".*/\1/')"

info "Current versions:"
info "  Cargo.toml (workspace): $CURRENT_CARGO_VERSION"
info "  kalla-web/package.json: $CURRENT_PKG_VERSION"

if [[ "$DRY_RUN" == true ]]; then
    warn "[dry-run] Would update Cargo.toml workspace.package.version: $CURRENT_CARGO_VERSION -> $VERSION"
    warn "[dry-run] Would update kalla-web/package.json version: $CURRENT_PKG_VERSION -> $VERSION"
else
    info "Updating Cargo.toml workspace.package.version to $VERSION ..."

    # Use sed to replace the version line inside [workspace.package].
    # We match the first 'version = "..."' line that appears after [workspace.package].
    # macOS sed requires slightly different syntax, so we use a portable approach.
    if sed --version >/dev/null 2>&1; then
        # GNU sed
        sed -i "s/^version = \"$CURRENT_CARGO_VERSION\"/version = \"$VERSION\"/" "$CARGO_TOML"
    else
        # BSD sed (macOS)
        sed -i '' "s/^version = \"$CURRENT_CARGO_VERSION\"/version = \"$VERSION\"/" "$CARGO_TOML"
    fi

    info "Updating kalla-web/package.json version to $VERSION ..."

    # Use sed to replace the "version" field in package.json.
    if sed --version >/dev/null 2>&1; then
        sed -i "s/\"version\": \"$CURRENT_PKG_VERSION\"/\"version\": \"$VERSION\"/" "$PACKAGE_JSON"
    else
        sed -i '' "s/\"version\": \"$CURRENT_PKG_VERSION\"/\"version\": \"$VERSION\"/" "$PACKAGE_JSON"
    fi

    # Verify the updates took effect
    UPDATED_CARGO="$(grep -m1 '^version' "$CARGO_TOML" | sed 's/.*"\(.*\)".*/\1/')"
    UPDATED_PKG="$(grep -m1 '"version"' "$PACKAGE_JSON" | sed 's/.*: *"\(.*\)".*/\1/')"

    if [[ "$UPDATED_CARGO" != "$VERSION" ]]; then
        error "Failed to update Cargo.toml. Expected $VERSION, got $UPDATED_CARGO"
        exit 1
    fi
    if [[ "$UPDATED_PKG" != "$VERSION" ]]; then
        error "Failed to update package.json. Expected $VERSION, got $UPDATED_PKG"
        exit 1
    fi

    info "Versions updated successfully."
fi

# ---------------------------------------------------------------------------
# Step 7: Build Docker images
# ---------------------------------------------------------------------------
info "Building Docker images with docker compose ..."

if [[ "$DRY_RUN" == true ]]; then
    warn "[dry-run] Would run: docker compose build"
else
    docker compose build
    info "Docker images built successfully."
fi

# ---------------------------------------------------------------------------
# Step 8: Create release commit and tag
# ---------------------------------------------------------------------------
info "Creating release commit and tag ..."

if [[ "$DRY_RUN" == true ]]; then
    warn "[dry-run] Would stage: Cargo.toml kalla-web/package.json"
    warn "[dry-run] Would commit with message: release: v$VERSION"
    warn "[dry-run] Would create tag: v$VERSION"
else
    git add Cargo.toml kalla-web/package.json

    # Also stage Cargo.lock if it was updated by the version change
    if [[ -n "$(git diff --name-only Cargo.lock 2>/dev/null)" ]]; then
        git add Cargo.lock
    fi

    git commit -m "release: v$VERSION"
    git tag -a "v$VERSION" -m "Release v$VERSION"

    info "Commit and tag created."
fi

# ---------------------------------------------------------------------------
# Step 9: Print push instructions
# ---------------------------------------------------------------------------
echo ""
printf "${GREEN}${BOLD}============================================${RESET}\n"
printf "${GREEN}${BOLD}  Release v$VERSION prepared successfully!${RESET}\n"
printf "${GREEN}${BOLD}============================================${RESET}\n"
echo ""
info "To publish this release, run the following commands:"
echo ""
echo "    git push origin main"
echo "    git push origin v$VERSION"
echo ""

if [[ "$DRY_RUN" == true ]]; then
    echo ""
    warn "This was a dry run. No changes were made."
    echo ""
fi
