---
name: data-lifecycle
description: Umbrella skill for running a data project end-to-end through its lifecycle stages — discover sources → profile the data → architect the platform → build the medallion pipeline → refactor the code. Use this whenever the user is kicking off a new data project, asks "where do I start" or "what are the steps", or is somewhere mid-lifecycle and unsure which stage skill applies. This skill ROUTES to the stage sub-skills (data-sourcing, data-profiling, data-architecture, medallion-design, notebook-refactor) and sequences them, pulling in the right one for the user's current stage.
origin: grouped
---

# Data Project Lifecycle (Router)

This is a **router skill** covering a data project from "we have an objective" to "the code is clean." Figure out which stage the user is in, then **invoke the matching stage skill with the Skill tool**. A greenfield project runs the stages in order; a mid-flight project jumps to its stage.

## The lifecycle

| Stage | Question it answers | Invoke sub-skill |
|---|---|---|
| 1. Discover | What data do I need and where do I get it? | `data-sourcing` |
| 2. Profile | What does the data actually look like? (checks + ER) | `data-profiling` |
| 3. Architect | How should the whole platform be structured, on what infra, at what cost? | `data-architecture` |
| 4. Build | Design the bronze/silver/gold pipeline, gold-first | `medallion-design` |
| 5. Refactor | Make the resulting notebook/code reviewable | `notebook-refactor` |

## Routing rules

- **"New project / where do I start"** → begin at stage 1 (`data-sourcing`) and proceed in order, confirming each stage's output before advancing.
- **User already has data in hand** → start at stage 2 (`data-profiling`).
- **Sources + shape known, needs a plan** → stage 3 (`data-architecture`).
- **Architecture decided, ready to build layers** → stage 4 (`medallion-design`).
- **Working code that's messy** → stage 5 (`notebook-refactor`) — can run anytime, independent of the others.
- Invoke by name, e.g. `Skill(skill="data-architecture")`. For a task spanning stages, invoke each in sequence and carry the output forward.

## How this router relates to the others

The lifecycle *produces* work that the topic routers *deepen*:

- Stage 3 `data-architecture` delegates cloud + cost to → [[cloud-data-infra]]
- Stage 4 `medallion-design` defers pipeline mechanics to → [[data-pipelines]] and the model to → [[data-modeling]]
- Profiling & building both hand validation/governance to → [[data-reliability]]
- ML objectives branch to → [[ml-feature-engineering]]

Use **this router for "what stage am I in"**; use the **topic routers for "go deep on pipelines/modeling/reliability/cloud."**
