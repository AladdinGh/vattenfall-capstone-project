CREATE CATALOG IF NOT EXISTS vattenfall_dev;
CREATE SCHEMA IF NOT EXISTS vattenfall_dev.raw;
CREATE SCHEMA IF NOT EXISTS vattenfall_dev.refined;
CREATE SCHEMA IF NOT EXISTS vattenfall_dev.analytics;

CREATE VOLUME IF NOT EXISTS vattenfall_dev.raw.landing;
CREATE VOLUME IF NOT EXISTS vattenfall_dev.raw.checkpoints;
