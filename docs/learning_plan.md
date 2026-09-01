# Learning Plan: Staying Relevant & Earning More in the SA Tech Market

**Target profile:** Data / AI Engineer who owns the data layer behind AI systems
**Starting point:** Python (5+ yrs), Spark/PySpark (in progress), Databricks (familiar)
**Horizon:** ~12 months, part-time alongside work
**Strategy in one line:** Turn your existing Python + Spark base into a specialised, cloud-native data-engineering skill set with an AI layer on top — the exact combination SA employers report as hardest to fill, and the one that opens remote-for-overseas rates.

---

## Why this direction (the market case)

- **SA has a severe digital skills shortage** — roughly 118,000 unfilled digital roles and a ~37% vacancy rate. AI, data engineering, cloud, and cybersecurity are consistently the hardest roles to fill.
- **You're already 60% of the way there.** Python and Spark are the core of modern data engineering. You're not starting a new career — you're specialising an existing one, which is far faster and lower-risk.
- **The pay ceiling is high and rising:**
  - Data engineer (SA): avg ~R460k–R610k; senior ~R700k–R900k; top earners ~R2m
  - Cloud engineer (SA): avg ~R965k; senior ~R1.2m
  - The biggest premium goes to data engineers whose work explicitly feeds AI systems
- **The real earning lever: remote-for-overseas.** SA data/cloud engineers increasingly serve international clients while living locally, earning global rates against a rand cost base. This is the single fastest way to multiply income with these skills — and it's only accessible once your stack and proof-of-work are credible.
- **Azure-first is the SA-smart choice.** Most SA enterprises and banks (your likely client/employer pool — Nedbank, Standard Bank, Discovery, Capitec, FNB, Absa) run Microsoft stacks. Azure + Databricks is the highest-leverage cloud bet locally, with AWS as a strong second.

---

## The target stack

| Layer | Skills |
|---|---|
| Language | Python (have), advanced SQL |
| Processing | Spark / PySpark, Databricks, Delta Lake |
| Cloud | Azure (primary), AWS (secondary) |
| Orchestration | Azure Data Factory / Airflow |
| Transformation | dbt |
| Infrastructure | Terraform (IaC), CI/CD |
| AI layer | RAG pipelines, vector databases, embeddings, LLM evaluation, MLOps basics |

---

## The phased plan

### Phase 0 — Consolidate the foundations (Weeks 1–4)
**Focus:** Close the gaps under your existing skills before building higher.
- Advanced SQL: window functions, CTEs, query optimisation, execution plans
- Finish the end-to-end PySpark project (partitioning, shuffles, joins, broadcast joins, tuning)
- Engineering hygiene: Git workflows, testing (pytest), code structure for data pipelines
- **Project:** A clean, tested PySpark ETL pipeline on a realistic messy dataset, in a public repo
- **Earning impact:** Foundation — sets up everything after it

### Phase 1 — Databricks + Delta Lake to a professional standard (Weeks 5–10)
**Focus:** Go from "familiar" to "credible" on the platform SA enterprises actually pay for.
- Delta Lake deeply: ACID, time travel, schema evolution, medallion architecture (bronze/silver/gold)
- Databricks: jobs, workflows, cluster configuration, Unity Catalog, performance tuning
- Structured Streaming basics
- **Certification:** Databricks Certified Data Engineer Associate
- **Project:** Extend your Phase 0 pipeline into a full medallion-architecture lakehouse
- **Earning impact:** Moves you into the "data engineer" band proper (R460k+)

### Phase 2 — Cloud + the modern data stack (Weeks 11–20)
**Focus:** Become deployable and production-grade, not just notebook-capable.
- Azure data services: Data Factory, Data Lake Storage, Synapse, key vault
- dbt for transformation and testing
- Terraform for infrastructure-as-code
- CI/CD for data pipelines (Azure DevOps or GitHub Actions)
- **Certification:** Azure Data Engineer Associate (DP-700 / current equivalent)
- **Project:** A fully cloud-deployed, IaC-provisioned, CI/CD pipeline pulling from a real API into a lakehouse
- **Earning impact:** Senior data engineer band (R700k–R900k); makes you remote-for-overseas viable

### Phase 3 — The AI layer (Weeks 21–32)
**Focus:** Add the differentiator that commands the top premium.
- Embeddings and vector databases (pgvector, Pinecone, or Azure AI Search)
- RAG pipelines: retrieval logic, chunking, grounding LLMs in your own data
- LLM evaluation: building eval suites, measuring quality (the skill that separates hobbyists from professionals)
- MLOps basics: model deployment, monitoring, versioning
- **Project:** A RAG system built on data flowing through your Phase 2 pipeline — e.g. a domain-specific knowledge assistant. This ties your whole stack together into one portfolio centrepiece.
- **Earning impact:** Top internal pay bands; strongest remote-for-overseas positioning

### Phase 4 — Positioning & monetisation (Weeks 33–52, ongoing)
**Focus:** Convert skills into income.
- Polish 3 portfolio projects into public, documented repos with clear READMEs
- Write up what you built (LinkedIn / blog) — proof-of-work beats certificates in this market
- Set up an OfferZen profile; explore international remote platforms
- Fold your new stack into consulting/freelance offerings at a higher rate
- Target a role change or internal move if current comp lags the market
- **Earning impact:** This is where the compounding pays out

---

## Certifications worth the time (in order)

1. **Databricks Certified Data Engineer Associate** — directly relevant, SA enterprises value it. https://www.databricks.com/learn/certification/data-engineer-associate $200
2. **Azure Data Engineer Associate** — the local cloud sweet spot https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/?practice-assessment-type=certification $165
3. *(Optional later)* **AWS Certified Data Engineer** — broadens your remote-for-overseas reach. https://aws.amazon.com/certification/certified-data-engineer-associate $150

Certifications are a *credible signal*, not the goal. Employers hire on proof-of-work; certs get you past the first filter.

---

## How to stay relevant (ongoing habits)

- **Depth over breadth.** Master this coherent stack rather than chasing every new tool. Specialists out-earn tool-hoppers.
- **Build in public.** One documented project is worth more than five certificates in this market.
- **Follow the money layer.** Keep your data work tied to AI use cases — that's where the pay premium concentrates and stays.
- **Reassess every 6 months.** The AI tooling layer moves fast; the data-engineering foundation underneath it is stable. Invest most in the stable base, refresh the top layer regularly.

---

## Realistic salary progression (SA, 2026 rand)

| Stage | Typical range |
|---|---|
| Now (Python dev, mid) | market mid band |
| After Phase 1–2 (cloud data engineer) | R460k–R700k |
| After Phase 3 (senior, AI-adjacent) | R700k–R1.2m+ |
| Remote-for-overseas | global rates against a rand cost base — the real multiplier |

*Figures are 2026 SA market estimates and vary by employer, city, and negotiation. Treat them as direction, not promises.*
