# Relational Data Warehouse Framework

A lightweight, open-source framework for building modern data platforms on top of relational databases.

This project is based on a simple premise:

Many organizations do not need distributed compute to solve their data problems. They need structure, discipline, and good practices.

## Motivation

Over the past years, the data ecosystem has converged around large-scale platforms such as lakehouses, distributed engines, and cloud-native warehouses. While powerful, these solutions often introduce:

- unnecessary infrastructure complexity
- high operational and compute costs
- overengineering for small-to-medium workloads

At the same time, many companies operate with:

- moderate data volumes
- limited concurrency
- straightforward analytical needs
- underdeveloped data governance

In these contexts, traditional relational databases are fully capable of supporting analytical workloads.

What is often missing is not compute, but a clear and opinionated way to build a data warehouse correctly.

## What This Is

This project is:

- a framework for disciplined data engineering
- a way to build reliable data warehouses on relational databases
- a tool for teams that value structure over scale

## What This Is Not

This project is not:

- a replacement for large-scale distributed systems
- a solution for petabyte-scale data processing
- a full-featured data platform with notebooks, ML tooling, or streaming engines

## Target Use Cases

This framework is designed for:

- small to medium-sized data teams
- companies with moderate data volumes
- teams adopting their first data warehouse
- environments where cost and simplicity matter
- projects that need governance without heavy infrastructure

## Project Goal

This project aims to provide a framework for building well-structured, governed data warehouses using relational databases, without requiring distributed systems.

It focuses on bringing modern data engineering practices into simpler environments.

## Core Principles

### 1. Contracts First

Every dataset should be defined by a data contract, which describes:

- structure and types
- expected semantics
- ownership and responsibility
- data quality expectations

This ensures that data is predictable, testable, and maintainable.

### 2. Built-in Data Quality

Data quality is not an afterthought.

The framework enforces:

- validation before and after data loads
- consistency checks over time
- explicit expectations about data behavior

The goal is to make data trustworthy by default.

### 3. Explicit Data Lifecycles

Data transformations should follow clear, well-defined stages, evolving from raw ingestion to curated datasets.

This promotes:

- clarity in data maturity
- separation of concerns
- easier debugging and lineage tracking

### 4. Standardized Write Patterns

Data ingestion and transformation should follow a small set of well-defined strategies, such as:

- full refreshes
- incremental updates
- append-only ingestion
- historical tracking

This avoids ad-hoc pipelines and improves consistency across datasets.

### 5. Dimensional Modeling as a Foundation

The framework encourages analytical modeling best practices, including:

- clear definition of data granularity
- separation between facts and dimensions
- consistent naming and semantic conventions

This ensures that data remains usable and understandable for analytics.

### 6. Simplicity Over Abstraction

The framework avoids unnecessary complexity:

- no reliance on distributed compute
- no requirement for proprietary platforms
- minimal operational overhead

The focus is on clarity, maintainability, and correctness.

## Long-Term Vision

The goal is to provide a minimal, extensible foundation for:

- building structured data platforms
- teaching good data engineering practices
- enabling teams to grow without premature complexity

As data needs evolve, users should be able to:

- extend the framework
- integrate with other tools
- migrate to more scalable systems without losing foundational discipline

## Contributing

This is an open-source project. Contributions are welcome in the form of:

- ideas and discussions
- documentation improvements
- use case reports
- feature proposals
