---
name: changelog-entry
description: Write the changelog entry for an Objectstore PR, or exclude a PR from the changelogs. Use when opening a PR, writing its title or description, or adding a `### Changelog Entry` section.
---

# Changelog Entries

The Python client, the Rust client and the server are released separately as
Craft workspaces (see `.craft.yml`), each with its own changelog:

- `clients/python/CHANGELOG.md`
- `clients/rust/CHANGELOG.md`
- `objectstore-server/CHANGELOG.md`, which also covers `objectstore-service`
  (`includePaths` in `.craft.yml`)

Craft lists a merged PR in the changelog of every workspace whose files it
changes, and adds the author and a link to the PR.

## Deciding on the Entry

1. **Exclude PRs that users won't notice**, for example changes to tests, CI,
   internal refactors, or a fix for a bug that was never released. To exclude
   one, write `#skip-changelog` anywhere in the PR description. Craft ignores
   the marker inside code spans and code blocks. PRs that only change files
   outside all workspaces are never listed and need no marker.
2. **Use the PR title if it's a good entry.** Craft groups the PR by the
   conventional commit type of its title (`feat`, `fix`, ...), and uses the
   rest of the title as the entry text. If the title follows the rules below
   and needs no details, there's nothing more to do.
3. **Otherwise, add a `### Changelog Entry` section** at the end of the PR
   description, after the description and any references such as
   `Fixes FS-123`, following the rules below. It replaces the title in every
   changelog the PR is listed in.

## Writing the Entry

- Write one top-level bullet, about a line long. Put relevant details in
  sub-bullets, only when they're needed.
- Describe the change in behavior that a user of the library notices, not the
  implementation or the problem statement from the PR description. Say what
  the library does now, not which problem no longer happens.
- For a breaking change or deprecation, add a sub-bullet with the migration
  step.
- The entry appears in every changelog the PR touches, so it must make sense to
  the readers of each. If it can't, suggest to split the PR.
