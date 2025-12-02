# Airflow Demo (Postgres Only)

This project now ships a pure PostgreSQL demo so you can explore Airflow 3.1
without touching SQL Server or SSIS. The stack provisions:

- `postgres-source`: transactional schema with sample e-commerce orders.
- `postgres-target`: analytics warehouse with `dwh` and `etl` schemas.
- Full Airflow 3 services (API server, scheduler, triggerer, DAG processor).

## Run the stack

```bash
docker compose up --build
```

The compose file pre-configures Airflow connections via environment variables,
so no manual UI work is required. Once the containers finish booting, visit
`http://localhost:8080` (admin/admin).

## Pipeline

`dags/sales_etl_postgres_pipeline.py` extracts customers, products, and order
line items from the source Postgres instance, loads dimensions and facts into
`dwh.*`, refreshes a daily summary table, and records progress in `etl.etl_log`.
Sample data lives in `scripts/init-postgres-source.sql` and the warehouse schema
is created by `scripts/init-postgres.sql`.

Trigger the `sales_etl_postgres_to_postgres` DAG manually or let the schedule
drive it—every component stays inside Postgres.

