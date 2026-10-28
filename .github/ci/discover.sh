#!/usr/bin/env bash
# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0
#
# Decides what the CI workflow runs, from the changed files and the project
# conventions described in the repository AGENTS.md. It names no project.
#
# A project is a top-level, non-hidden directory. A change touches a project
# when it changes a file inside it; a change anywhere else (the workflow, root
# files) touches every project.
#
# Environment:
#   BASE     commit to compare with: the pull request base or the commit before
#            a push. Empty or all zeros (a new branch) compares with HEAD's parent.
#   HEAD     commit under test (default: HEAD)
#   PUBLISH  "true" when images may be published (pushes to main)
#
# Outputs, as name=JSON lines to $GITHUB_OUTPUT (stdout when unset):
#   checks   [{"project": dir, "ci": bool, "integration": bool}] for touched
#            projects whose Makefile declares a `ci` or `ci-integration` target
#   images   [dir] touched projects with a Dockerfile whose .version changed
#
# Local dry run: BASE=<commit> HEAD=<commit> PUBLISH=true .github/ci/discover.sh

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
head=${HEAD:-HEAD}
base=${BASE:-}

if [[ -n $base && ! $base =~ ^0+$ ]] && git cat-file -e "$base^{commit}" 2>/dev/null; then
  # Three dots: changes on HEAD since the merge base, also after a force push.
  # No rename detection: a moved file touches the project it left as well.
  changed=$(git diff --no-renames --name-only "$base...$head")
else
  changed=$(git diff-tree --no-renames --root --no-commit-id --name-only -r "$head")
fi

has_target() { # <project> <target>: the Makefile declares the target literally
  [[ -f $1/Makefile ]] && grep -Eq "^$2:([^=]|$)" "$1/Makefile"
}

declare -A touched=() version_changed=()
shared=false
while IFS= read -r file; do
  [[ -n $file ]] || continue
  top=${file%%/*}
  if [[ $file != */* || $top == .* ]]; then
    shared=true
  elif [[ -d $top ]]; then
    touched[$top]=1
    [[ $file == "$top/.version" ]] && version_changed[$top]=1
  fi
done <<<"$changed"

projects=()
for dir in */; do
  dir=${dir%/}
  if $shared || [[ -n ${touched[$dir]:-} ]]; then
    projects+=("$dir")
  fi
done

checks=$(
  for project in "${projects[@]}"; do
    ci=false integration=false
    has_target "$project" ci && ci=true
    has_target "$project" ci-integration && integration=true
    if $ci || $integration; then
      jq -cn --arg project "$project" --argjson ci "$ci" --argjson integration "$integration" \
        '{project: $project, ci: $ci, integration: $integration}'
    fi
  done | jq -cs .
)

images=$(
  if [[ ${PUBLISH:-false} == true ]]; then
    for project in "${!version_changed[@]}"; do
      if [[ -f $project/Dockerfile && -f $project/.version ]]; then
        printf '%s\n' "$project"
      fi
    done
  fi | sort | jq -Rcs 'split("\n") | map(select(length > 0))'
)

{
  echo "checks=$checks"
  echo "images=$images"
} >>"${GITHUB_OUTPUT:-/dev/stdout}"

if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  {
    echo "### CI plan"
    echo "Shared files changed: $shared"
    echo "Checks: $(jq -r 'map(.project + (if .integration then " (+integration)" else "" end)) | join(", ")' <<<"$checks")"
    echo "Images: $(jq -r 'join(", ")' <<<"$images")"
  } >>"$GITHUB_STEP_SUMMARY"
fi
