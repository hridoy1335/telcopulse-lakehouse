# TelcoPulse Lakehouse

### Real-Time Telecom Data Engineering Platform

**From Operational Data to Real-Time Network Intelligence.**

TelcoPulse is a portfolio-driven data engineering project designed to build an end-to-end telecom analytics platform using modern cloud and big data technologies.

The platform combines relational operational data from Neon PostgreSQL with continuous network telemetry events from Azure Event Hubs, processing both batch and streaming workloads through Azure Databricks.

### Project Objectives

- Build reliable batch ingestion from relational PostgreSQL sources.
- Ingest continuous network usage events through Kafka-compatible Azure Event Hubs.
- Implement a scalable Bronze, Silver, and Gold Lakehouse architecture.
- Apply data quality, deduplication, and incremental processing.
- Govern analytical data using Unity Catalog.
- Automate DEV, QA, and PROD deployments using Databricks Asset Bundles and GitHub Actions.
- Implement secretless CI/CD authentication using Microsoft Entra ID and Databricks OIDC federation.

### Technology Stack

|    | Category            | Technologies                                       |
| -- | ------------------- | -------------------------------------------------- |
| 1  | Relational Database | Neon PostgreSQL                                    |
| 2  | Streaming           | Apache Kafka Protocol, Azure Event Hubs            |
| 3  | Cloud Platform      | Microsoft Azure                                    |
| 4  | Data Engineering    | Azure Databricks, Apache Spark, PySpark, Spark SQL |
| 5  | Storage             | Azure Data Lake Storage Gen2                       |
| 6  | Table Format        | Delta Lake                                         |
| 7  | Governance          | Unity Catalog                                      |
| 8  | Deployment          | Databricks Asset Bundles                           |
| 9  | CI/CD               | GitHub Actions                                     |
| 10 | Authentication      | Microsoft Entra ID OIDC, Databricks OIDC           |

**Project Status:** In Development
