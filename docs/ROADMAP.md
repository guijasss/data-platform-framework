# Roadmap

This roadmap is for building the framework described in `FRAMEWORK.md`, using the open questions in `QUESTIONS.md` plus one important architectural decision already made:

* transformation logic should remain explicit and Spark-native
* the framework should provide `Reader` / `Writer` abstractions, validation, config, contracts, and runtime services
* avoid a large `Orchestrator` that hides pipeline behavior behind callbacks or a custom DSL

The development strategy should be: define stable abstractions first, implement the minimum usable runtime second, and automate only after those abstractions are proven.

## Core product decisions

Before implementation expands, these decisions should be treated as the framework baseline:

* contract-first development
    * every table must have a data contract before pipeline code exists
* explicit ETL flow
    * client code builds the flow in plain PySpark
    * framework handles repetitive infrastructure concerns
* Delta Lake plus medallion architecture as first-class assumptions
* UTC-only timestamps
* three write modes only
    * `APPEND`
    * `INCREMENTAL`
    * `FULL_OVERWRITE`
* bronze and silver preserve `{system}_{entity}` granularity
* silver allows transformations but no joins
* gold is divided into four subdomains
    * dimensional
    * feature store
    * metrics / okrs
    * model output

## Target architecture

The framework should converge to a structure like this:

* `PipelineContext`
    * Spark session
    * resolved config
    * table contract
    * logger / metrics emitter
    * reader
    * writer
* `Reader`
    * source loading
    * layer-to-layer reading
    * schema-aware loading hooks
* `Writer`
    * Delta write strategy
    * merge / overwrite / append behavior
    * standard columns handling
    * validation before write
* `Contract`
    * table metadata
    * schema
    * PK and constraints
    * write mode
    * ownership and quality rules
* `Validators`
    * contract validation
    * schema validation
    * write-safety validation
* `Lineage`
    * optional metadata emission from reads and writes
    * later integration with `graphliterate`-style lineage storage if kept in scope

Client pipelines should look more like normal code than framework configuration:

```python
def run(context: PipelineContext) -> None:
    df = context.reader.read_source("salesforce", "customers")
    df = transform(df)
    context.writer.write_table(df=df, contract=context.contract)
```

That keeps business logic obvious and keeps the framework focused on platform concerns.

## Development phases

## Phase 1: Specification

Goal: remove ambiguity before writing framework internals.

### Deliverables
* define the project name and one-sentence scope
* write a data contract specification
* write a pipeline structure specification
* write a config specification
* define the gold-layer taxonomy
* define the minimum lineage model

### Decisions to make in this phase
* exact contract file format
    * YAML is the most natural default here
* what is global config vs table-local config
* whether lineage is part of `v0` metadata or a post-`v0` extension
* how strict contract validation should be for bronze

### Suggested outputs
* `docs/rfcs/001-data-contract.md`
* `docs/rfcs/002-pipeline-structure.md`
* `docs/rfcs/003-configuration.md`
* `docs/rfcs/004-lineage.md`

### Exit criteria
* no major architectural question remains unresolved
* one canonical example contract exists for bronze, silver, and gold

## Phase 2: Skeleton and Packaging

Goal: create the real framework layout in code.

### Deliverables
* create source package layout under `src/`
* separate framework code from example pipelines
* define module boundaries for context, contracts, readers, writers, validators, and config
* replace the current toy example with a representative framework-driven example

### Suggested layout
* `src/framework/context.py`
* `src/framework/contracts/`
* `src/framework/config/`
* `src/framework/io/readers/`
* `src/framework/io/writers/`
* `src/framework/validators/`
* `src/framework/lineage/`
* `src/pipelines/bronze/`
* `src/pipelines/silver/`
* `src/pipelines/gold/`

### Exit criteria
* repository structure matches the conceptual architecture
* a developer can see where contracts, pipeline code, and framework internals belong

## Phase 3: Contract System

Goal: implement the contract-first backbone of the framework.

### Deliverables
* contract parser and schema loader
* required field validation
* PK definition support
* standard metadata fields
    * layer
    * system
    * entity
    * owner
    * description
    * write mode
* quality rule declarations
* contract-to-runtime object conversion

### Exit criteria
* invalid contracts fail before Spark execution starts
* contract objects can drive writer behavior and validation

## Phase 4: Reader and Writer Runtime

Goal: make the framework actually execute pipelines.

### Deliverables
* implement `PipelineContext`
* implement `Reader`
    * source reads
    * table reads
    * layer-aware reads
* implement `Writer`
    * append
    * incremental merge
    * full overwrite
* implement standard column handling
    * `id`
    * `processed_at`
* implement UTC timestamp normalization
* implement source-column sanitization for Delta-incompatible column names

### Exit criteria
* one bronze pipeline can read and write through framework primitives
* one silver pipeline can read bronze output, transform, and write through the same runtime

## Phase 5: Validation and Safety

Goal: enforce framework rules consistently.

### Deliverables
* schema compatibility checks
* missing-column checks
* duplicate PK checks where applicable
* invalid write-mode checks
* silver no-join policy definition
    * at minimum, document it
    * optionally add static or runtime safeguards later
* pre-write validation hooks
* post-write metadata summary

### Exit criteria
* framework fails early on invalid writes
* write behavior is deterministic and contract-driven

## Phase 6: Examples and Developer Experience

Goal: make the framework usable by other engineers.

### Deliverables
* one realistic bronze example
* one realistic silver example
* one realistic gold example
* scaffolding template for adding a new table
* documentation for adding a source, contract, and pipeline
* opinionated conventions for `{system}_{entity}` folder structure

### Exit criteria
* a new engineer can create a table without reading framework internals
* examples demonstrate the intended ETL flow clearly

## Phase 7: Testing

Goal: make evolution safe.

### Deliverables
* unit tests for contracts, config, and validators
* integration tests for Spark and Delta writes
* end-to-end tests for bronze-to-silver flow
* fixture datasets for common bad cases
    * duplicate PK
    * schema drift
    * missing required fields
    * invalid timestamps
* stress-test plan
    * increasing input volume
    * execution-time tracking
    * resource observations

### Exit criteria
* framework behavior is testable without manual validation
* at least one performance baseline exists for incremental writes

## Phase 8: Metadata, Lineage, and Observability

Goal: make pipelines visible instead of invisible.

### Deliverables
* runtime execution logs
* per-run metadata
    * table
    * layer
    * write mode
    * rows written
    * execution time
* read/write event capture for lineage
* evaluate `graphliterate`-style lineage integration
    * if it adds low-friction value, keep it
    * if it complicates `v0`, emit neutral metadata first and integrate later

### Exit criteria
* every pipeline run emits enough metadata to reconstruct what happened
* lineage design does not distort the core pipeline API

## Phase 9: Plug'n'Play and Catalog Support

Goal: make the framework adaptable to different platform setups.

### Deliverables
* catalog abstraction points
* local and cloud environment support
* support for alternate metastores or table catalogs
* environment-aware configuration resolution

### Exit criteria
* catalog integration is configurable without rewriting business pipelines

## Phase 10: CI/CD and Deployment

Goal: automate stable behavior, not unstable ideas.

### Deliverables
* lint and test pipeline in CI
* contract validation in CI
* packaging and versioning strategy
* explicit DAG registration model
    * avoid “commit code and job appears automatically” in the first version
    * prefer explicit pipeline registration derived from validated metadata
* environment promotion workflow

### Exit criteria
* pull requests validate framework integrity automatically
* orchestration registration is explicit and reproducible

## Recommended `v0`

The first usable version should be intentionally small.

### Include in `v0`
* contract model
* config model
* `PipelineContext`
* `Reader`
* `Writer`
* three write modes
* standard column enforcement
* UTC enforcement
* schema and write validation
* one bronze example
* one silver example
* unit and integration tests
* basic run metadata

### Exclude from `v0`
* advanced gold modeling helpers
* feature store abstractions
* automatic DAG creation from git changes
* deep lineage UI or graph backend dependence
* broad plug'n'play catalog support beyond one clean extension point

## Immediate next steps

1. write the data contract RFC and choose its file format
2. write the ETL flow RFC around `PipelineContext`, `Reader`, and `Writer`
3. define repository and package layout under `src/`
4. implement contract parsing and validation first
5. implement `Writer` with the three write modes
6. implement `Reader` and a bronze example pipeline
7. add a silver example pipeline using explicit DataFrame transformations
8. add tests before expanding into gold, lineage integrations, or CI/CD automation
