# 🔧 Setup Guide

Complete guide to set up the AWS ETL Pipeline project for local development and AWS deployment.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [AWS Configuration](#aws-configuration)
4. [Environment Variables](#environment-variables)
5. [Running Locally](#running-locally)
6. [Deploying to AWS](#deploying-to-aws)
7. [Troubleshooting](#troubleshooting)

---

## 🔍 Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| **Python** | 3.9+ | Main language |
| **pip** | Latest | Package management |
| **AWS CLI** | 2.x | AWS interaction |
| **Docker** | 20.x+ | Local Spark/Glue testing |
| **Git** | 2.x+ | Version control |
| **Terraform** | 1.5+ | Infrastructure as Code _(optional)_ |

### AWS Requirements

- AWS Account with permissions for:
  - S3 (read/write)
  - Lambda (create/invoke)
  - Glue (jobs, crawlers, catalog)
  - Athena (query)
  - CloudWatch (logs)
  - IAM (roles/policies)

---

## 🏠 Local Development Setup

### 1. Clone Repository

git clone https://github.com/ItalloLinhares/aws-etl-olist.git
cd aws-etl-olist

2. Create Virtual Environment
Linux/Mac:
python3 -m venv venv
source venv/bin/activate

Windows:
python -m venv venv
venv\Scripts\activate

3. Install Dependencies
# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt

# Verify installation
python -c "from pyspark.sql import SparkSession; print('PySpark OK!')"
4. Install Development Dependencies
bash
Copy code
pip install -r requirements-dev.txt
requirements-dev.txt:

pytest==7.4.0
pytest-cov==4.1.0
pytest-spark==0.6.0
chispa==0.9.2
black==23.7.0
flake8==6.1.0
mypy==1.5.0
ipython==8.14.0
jupyter==1.0.0
