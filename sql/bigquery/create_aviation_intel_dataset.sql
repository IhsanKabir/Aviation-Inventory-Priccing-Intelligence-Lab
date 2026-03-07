-- Concrete BigQuery dataset bootstrap for Aero Pulse Intelligence.
-- Project: aeropulseintelligence
-- Dataset: aviation_intel

CREATE SCHEMA IF NOT EXISTS `aeropulseintelligence.aviation_intel`
OPTIONS (
  description = 'Curated aviation intelligence warehouse for operational analytics, BI, and thesis reporting'
);
