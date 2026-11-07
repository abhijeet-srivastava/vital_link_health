# PostgreSQL 16 with pg_partman for daily partitions of reading, alert_occurrence and audit_log.
# btree_gist (EXCLUDE constraints on device_binding) ships with the contrib modules in the base image.
FROM postgres:16-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16-partman \
 && rm -rf /var/lib/apt/lists/*
