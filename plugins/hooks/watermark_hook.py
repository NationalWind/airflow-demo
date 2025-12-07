"""
Custom Hook for managing ETL watermarks (LSET/CET).
"""

from datetime import datetime
from typing import Optional, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook


class WatermarkHook(PostgresHook):
    """
    Hook for managing ETL watermarks in metadata database.
    """

    def __init__(self, postgres_conn_id: str = 'postgres_metadata', *args, **kwargs):
        super().__init__(postgres_conn_id=postgres_conn_id, *args, **kwargs)

    def get_watermark(
        self,
        dag_id: str,
        source_system: str,
        source_table: str,
        target_layer: str
    ) -> Tuple[Optional[datetime], datetime]:
        """
        Get LSET (Last Successful Execution Time) and CET (Current Execution Time).

        Args:
            dag_id: Airflow DAG ID
            source_system: Source system name (crm, erp, web)
            source_table: Source table name
            target_layer: Target layer (stg, nds, dds)

        Returns:
            Tuple of (LSET, CET). LSET is None for first run.
        """
        sql = """
            SELECT lset, CURRENT_TIMESTAMP AS cet
            FROM metadata.etl_watermark
            WHERE dag_id = %s
              AND source_system = %s
              AND source_table = %s
              AND target_layer = %s
              AND last_run_status = 'SUCCESS'
        """

        result = self.get_first(sql, parameters=(dag_id, source_system, source_table, target_layer))

        if result:
            return result[0], result[1]
        else:
            # First run - return None for LSET, current timestamp for CET
            cet_result = self.get_first("SELECT CURRENT_TIMESTAMP")
            return None, cet_result[0]

    def update_watermark(
        self,
        dag_id: str,
        source_system: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        lset: Optional[datetime],
        cet: datetime,
        status: str,
        rows_extracted: int = 0,
        rows_loaded: int = 0
    ) -> None:
        """
        Update watermark after successful/failed run.

        Args:
            dag_id: Airflow DAG ID
            source_system: Source system name
            source_table: Source table name
            target_layer: Target layer
            target_table: Target table name
            lset: Last Successful Execution Time (from get_watermark)
            cet: Current Execution Time (from get_watermark)
            status: Run status (SUCCESS, FAILED)
            rows_extracted: Number of rows extracted
            rows_loaded: Number of rows loaded
        """
        sql = """
            INSERT INTO metadata.etl_watermark (
                dag_id, source_system, source_table, target_layer, target_table,
                lset, cet, last_run_start, last_run_end, last_run_status,
                rows_extracted, rows_loaded, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (dag_id, source_system, source_table, target_layer)
            DO UPDATE SET
                lset = EXCLUDED.cet,  -- CET becomes next run's LSET if successful
                cet = EXCLUDED.cet,
                last_run_start = EXCLUDED.last_run_start,
                last_run_end = EXCLUDED.last_run_end,
                last_run_status = EXCLUDED.last_run_status,
                rows_extracted = EXCLUDED.rows_extracted,
                rows_loaded = EXCLUDED.rows_loaded,
                updated_at = CURRENT_TIMESTAMP
        """

        self.run(
            sql,
            parameters=(
                dag_id, source_system, source_table, target_layer, target_table,
                lset, cet, lset if lset else cet, cet, status,
                rows_extracted, rows_loaded
            )
        )

    def log_etl_run(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        source_system: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        execution_date: str,
        status: str,
        lset: Optional[datetime] = None,
        cet: Optional[datetime] = None,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        error_message: str = None
    ) -> int:
        """
        Log ETL run details to run log table.

        Returns:
            run_id: ID of the logged run
        """
        sql = """
            INSERT INTO metadata.etl_run_log (
                dag_id, dag_run_id, task_id, source_system, source_table,
                target_layer, target_table, execution_date, start_time, status,
                lset_used, cet_used, rows_extracted, rows_loaded, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """

        result = self.get_first(
            sql,
            parameters=(
                dag_id, dag_run_id, task_id, source_system, source_table,
                target_layer, target_table, execution_date, status,
                lset, cet, rows_extracted, rows_loaded, error_message
            )
        )

        return result[0] if result else None

    def get_last_successful_run(
        self,
        dag_id: str,
        source_system: str,
        source_table: str,
        target_layer: str
    ) -> Optional[dict]:
        """
        Get details of last successful run.

        Returns:
            Dictionary with watermark details or None
        """
        sql = """
            SELECT
                watermark_id, lset, cet, last_run_start, last_run_end,
                rows_extracted, rows_loaded, updated_at
            FROM metadata.etl_watermark
            WHERE dag_id = %s
              AND source_system = %s
              AND source_table = %s
              AND target_layer = %s
              AND last_run_status = 'SUCCESS'
        """

        result = self.get_first(sql, parameters=(dag_id, source_system, source_table, target_layer))

        if result:
            return {
                'watermark_id': result[0],
                'lset': result[1],
                'cet': result[2],
                'last_run_start': result[3],
                'last_run_end': result[4],
                'rows_extracted': result[5],
                'rows_loaded': result[6],
                'updated_at': result[7]
            }
        return None
