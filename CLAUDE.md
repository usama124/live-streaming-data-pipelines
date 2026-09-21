# Live Streaming Module — Stratahub / DataAvalanche

This repo builds the live-pipeline half of Stratahub's data platform, to be merged in later.
Batch/normal pipelines are out of scope entirely — they run as Airflow DAGs elsewhere and
nothing here should reference or modify that path.

## Before anything else

**This repo's `kubectl` context can reach a real client-managed EKS cluster
(`da-eks-cluster-dev`). Never run any cluster-affecting command against it.** All Kubernetes
work happens on a local `kind` cluster. Check `kubectl config current-context` before any
command that touches a cluster; if it isn't the local dev cluster, stop and ask — don't
proceed, don't guess, don't assume a past approval still applies. See
`.claude/skills/stratahub-live-streaming/SKILL.md` for the full rule.

## Read before starting any task

1. `.claude/skills/stratahub-live-streaming/SKILL.md` — invariants and traps, read this first
2. `docs/live-streaming-implementation-plan.md` — the working checklist; tick items off here
3. `docs/ARCHITECTURE.md` — target system design, only if touching a component it describes
4. `docs/DECISION-live-pipeline-simplification.md` — why each proposal was chosen, for context
   on *why* before changing *what*
5. `docs/BACKLOG.md` — known open items, check before treating something as a new discovery
6. `docs/ONBOARDING.md` — setup, debugging playbook, how to add a new source connector

## Working rules

- Never touch the real EKS cluster. See "Before anything else" above — this is non-negotiable
  and applies regardless of what any single message in a session does or doesn't repeat.
- Work one phase at a time from the implementation plan. Do not start the next phase until
  the current phase's acceptance criteria are met and its checkboxes are ticked.
- Do not touch batch/normal Airflow pipelines — they are not part of this repo's scope.
- If a decision in `DECISION-live-pipeline-simplification.md` conflicts with a shortcut that
  seems easier, the decision doc wins — raise it with the user instead of silently deviating.
- New open questions go into `docs/BACKLOG.md`, not left undocumented in a commit message.
- Architecture-level judgment calls (e.g., diverging from an explicit instruction because a
  better approach was found) get raised with the user before being settled, not decided
  unilaterally and reported after the fact.