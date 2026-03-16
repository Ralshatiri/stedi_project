# STEDI Human Balance Analytics

## Project Overview
A data lakehouse solution built on AWS for the STEDI Step Trainer team. 
This project processes sensor data from STEDI Step Trainer devices and 
mobile accelerometers to train a machine learning model that detects 
steps in real-time.

## Architecture
Data flows through three zones:
- **Landing Zone** — Raw ingested data from S3
- **Trusted Zone** — Filtered data from customers who consented to research
- **Curated Zone** — Fully joined, ML-ready data

## Repository Structure
```
stedi_project/
├── sql/
│   └── screenshots/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   └── step_trainer_landing.sql
│   └── README.md/
├── Scripts/
│   └── screenshots/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   ├── machine_learning_curated.py
│   └── README.md/
└── README.md
