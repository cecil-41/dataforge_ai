# DataForge AI — Project Instructions

## Mission
Building **DataForge AI**: one evolving, production-style data + AI platform
(PySpark → Databricks/Delta Lake → Azure → AI/RAG layer), used to teach modern
data engineering end-to-end. Full phase roadmap:
[docs/learning_plan.md](../docs/learning_plan.md). Current progress:
[docs/roadmap_progress.md](../docs/roadmap_progress.md).

## Baseline skills (do not re-teach from scratch)
Python, SQL fundamentals, AWS, Terraform, backend/APIs, Git, Docker, basic
Databricks familiarity. Briefly assess, then go straight to
professional/production-level usage.

## How to teach new concepts
For every new concept: explain what it is and why it matters professionally,
citing the official doc/source link → show where it fits in DataForge AI's
architecture → small example → a hands-on task I attempt myself first →
integrate it into DataForge AI → verify it works → brief recap + a small
challenge → update [docs/roadmap_progress.md](../docs/roadmap_progress.md).

I'm an experienced developer, not a Spark/cloud beginner at this point in
the project — keep explanations concise (assume I can fill in obvious
detail), skip the hint→stronger-hint escalation ladder, and default to
giving the doc source + a short "what it does" instead of lengthy
hand-holding. When a hands-on task comes up, just implement it directly
(full working code, not a skeleton) with a brief note on why it works —
optimize for speed and progress, not making me write it myself first.

For a deeper, structured mentoring session (phase kickoffs, full roadmap
context, progress review), use the **DataForge Mentor** custom agent
(`.github/agents/dataforge-mentor.agent.md`) instead of the default agent.

## Architecture decisions
Before major features: state the requirement, list 2–3 realistic approaches,
recommend one, state the trade-offs (cost, complexity, Azure/Databricks fit),
then implement. Azure is the primary cloud; AWS only for contrast. Prefer the
simplest architecture that demonstrates the professional concept — don't add
a technology just because it exists.

## Code standards
Aim for production quality: modular, type-hinted, config via env vars/config
files, tested (pytest), logged, secrets never hard-coded.

## Environment
- Local dev via Docker: `docker compose up -d` → JupyterLab at
  `localhost:8888` (token `pyspark`), Spark UI at `localhost:4040`.
- Dataset: NYC TLC Yellow Taxi trips in `data/raw/`, fetched via
  `scripts/download_data.py`.
- User always executes notebook cells in the JupyterLab browser UI, not via
  VS Code's notebook execution. Don't run cells with the notebook execution
  tool for this repo — just prepare/edit cells and let the user run them.
