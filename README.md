# 🏪 AWS ETL Pipeline - Olist E-commerce

> Serverless ETL pipeline for processing Brazilian e-commerce data with data quality validation and analytics

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-Cloud-orange.svg)](https://aws.amazon.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 📊 About The Project

This project implements a complete serverless ETL pipeline on AWS to process data from the Olist Brazilian e-commerce dataset (Kaggle). The pipeline performs:

- ✅ Automated data ingestion via Lambda
- ✅ Quality validation with 15+ custom rules
- ✅ Distributed processing with PySpark
- ✅ Transformation and optimization (Parquet format)
- ✅ Automatic cataloging with Glue Crawler
- ✅ SQL query availability (Athena)
- ✅ Interactive dashboards (Power BI)

## 🏗️ Architecture

![Architecture](docs/architecture-diagram.png)

**Data Flow:**
1. **Lambda Function**: Downloads dataset from Kaggle → S3 (raw)
2. **Glue Job**: Reads CSVs, validates quality, transforms → Parquet
3. **Glue Crawler**: Maps schemas → Glue Data Catalog
4. **Athena**: SQL queries over cataloged data
5. **Power BI**: Interactive dashboards

For detailed architecture, see [docs/architecture.md](docs/architecture.md)

## 🔧 Tech Stack

| Layer | Technology |
|-------|------------|
| **Cloud** | AWS (Lambda, S3, Glue, Athena, CloudWatch) |
| **Processing** | PySpark 3.x |
| **Language** | Python 3.9+ |
| **Data Format** | Parquet (Snappy compression) |
| **Logging** | structlog (structured logging) |
| **Testing** | pytest, chispa |
| **IaC** | Terraform _(in progress)_ |

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- AWS Account (with permissions for Lambda, S3, Glue, Athena)
- AWS CLI configured
- Docker (for local testing)

### Local Installation (for development)

# Clone repository
git clone https://github.com/ItalloLinhares/aws-etl-olist.git
cd aws-etl-olist

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt