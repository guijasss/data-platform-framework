Phase 1 — Project Skeleton (2–3 days)
Goal

Create a usable foundation.

Tasks
Initialize repository
Define structure:
/contracts
/pipelines
/framework
/docs
/examples
Choose interface style:
CLI-first (recommended)
Create basic CLI:
rdw init
rdw run pipeline_name
rdw validate contract.yaml
Setup:
packaging (poetry / pip)
config system (YAML-based)
Deliverable
CLI that runs and parses config files (even if no logic yet)
Phase 2 — Data Contracts (Core) (1–2 weeks)
Goal

Make contracts the center of the system.

Tasks
Define contract schema (YAML)
Implement:
parser
validation (schema correctness)
normalization
Generate:
SQL table DDL from contract
diff detection (schema changes)
Add CLI commands:
rdw contract validate path.yaml
rdw contract apply path.yaml
Stretch
versioning (basic)
contract linting rules
Deliverable
Contracts → tables reproducibly created from spec
Phase 3 — Write Engine (1–2 weeks)
Goal

Standardize how data enters tables.

Tasks

Implement write modes:

full_load
append
incremental
merge/upsert
scd2 (initial version)

Each mode should:

accept input dataset (CSV, query, dataframe)
apply deterministic logic
log execution
Key design constraint
Same interface for all modes:
write(dataset, contract, mode, context)
Deliverable
Deterministic loading engine
Phase 4 — Data Quality Engine (1–2 weeks)
Goal

Make validation automatic and contract-driven.

Tasks
Implement core checks:
not null
unique
accepted values / range
row count bounds
freshness (basic)
Generate SQL from contract rules
Execution flow:
before load → validate input (optional)
after load → validate table
Store results (log or table)
CLI
rdw quality run table_name
Deliverable
Automatic data validation tied to contracts
Phase 5 — Pipeline Execution (1 week)
Goal

Connect everything into a usable flow.

Tasks
Define pipeline spec:
pipeline: orders
steps:
  - extract
  - transform
  - load
Implement execution engine:
load contract
→ run transformation
→ apply write mode
→ run quality checks
→ log execution
Add:
rdw run pipeline.yaml
Deliverable
End-to-end pipeline execution
Phase 6 — Metadata & Observability (1–2 weeks)
Goal

Make system debuggable and inspectable.

Tasks

Track:

pipeline runs
load timestamps
row counts
quality results
failures

Expose:

CLI:
rdw runs list
rdw runs inspect <id>
Deliverable
Basic observability layer
Phase 7 — Modeling Guidelines (parallel, docs)
Goal

Guide users toward correct usage.

Tasks

Write documentation for:

how to define grain
how to model facts vs dimensions
when to use each write mode
how to structure transformations
Deliverable
docs/modeling.md
Phase 8 — Examples (critical)
Goal

Make the project understandable.

Tasks

Create a full example:

raw data (CSV or mock source)
contracts
pipelines
transformations
final tables

Example domains:

e-commerce (orders, customers, products)
SaaS metrics (events, users, subscriptions)
Deliverable
/examples/ecommerce/
Phase 9 — Developer Experience (1–2 weeks)
Goal

Reduce friction.

Tasks
Improve error messages
Add dry-run mode
Add config validation
Add template generators:
rdw generate contract fact_orders
rdw generate pipeline orders
Improve CLI UX
Phase 10 — Stabilization & Release
Goal

Make it usable by others.

Tasks
Version v0.1.0
Add:
installation guide
quickstart
minimal tutorial
Create:
README.md
docs/getting-started.md