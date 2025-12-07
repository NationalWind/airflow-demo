"""
SQL query templates for ETL operations.
"""

from typing import List


class SQLTemplates:
    """
    Collection of parameterized SQL templates for ETL operations.
    """

    @staticmethod
    def incremental_extract(
        source_table: str,
        columns: List[str],
        has_lset: bool = True
    ) -> str:
        """
        Generate SQL for incremental extraction using LSET/CET.
        Uses GREATEST(created_at, updated_at) to handle cases where updated_at might be NULL.

        Args:
            source_table: Fully qualified source table name (schema.table)
            columns: List of column names to select
            has_lset: Whether LSET exists (False for first run)

        Returns:
            SQL query string
        """
        columns_str = ', '.join(columns)

        if has_lset:
            return f"""
                SELECT {columns_str}
                FROM {source_table}
                WHERE GREATEST(COALESCE(updated_at, created_at), created_at) > %s
                  AND GREATEST(COALESCE(updated_at, created_at), created_at) <= %s
                ORDER BY GREATEST(COALESCE(updated_at, created_at), created_at)
            """
        else:
            return f"""
                SELECT {columns_str}
                FROM {source_table}
                WHERE GREATEST(COALESCE(updated_at, created_at), created_at) <= %s
                ORDER BY GREATEST(COALESCE(updated_at, created_at), created_at)
            """

    @staticmethod
    def truncate_staging(staging_table: str) -> str:
        """Generate SQL to truncate staging table."""
        return f"TRUNCATE TABLE {staging_table}"

    @staticmethod
    def insert_staging(
        staging_table: str,
        columns: List[str],
        source_system: str
    ) -> str:
        """
        Generate SQL for inserting into staging table.

        Args:
            staging_table: Staging table name
            columns: List of column names (excluding audit columns)
            source_system: Source system identifier

        Returns:
            INSERT SQL statement
        """
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        return f"""
            INSERT INTO {staging_table}
                ({columns_str}, stg_source_system, stg_loaded_at)
            VALUES ({placeholders}, '{source_system}', CURRENT_TIMESTAMP)
        """

    @staticmethod
    def scd_type2_insert(
        nds_table: str,
        business_columns: List[str],
        natural_key_columns: List[str],
        source_system: str
    ) -> str:
        """
        Generate SQL for SCD Type 2 insert (new records).

        Args:
            nds_table: NDS table name
            business_columns: List of business attribute columns
            natural_key_columns: Columns that form the natural key
            source_system: Source system identifier

        Returns:
            INSERT SQL for new records
        """
        # Build natural key concatenation
        nk_parts = [f"'{source_system}'"] + [f"CAST(stg.{col} AS VARCHAR)" for col in natural_key_columns]
        nk_concat = ' || \'-\' || '.join(nk_parts)

        # Build hash key columns (business attributes only)
        hash_columns = [f"COALESCE(CAST(stg.{col} AS VARCHAR), '')" for col in business_columns]
        hash_concat = ' || \'|\' || '.join(hash_columns)

        all_columns = natural_key_columns + business_columns
        stg_columns = ', '.join([f'stg.{col}' for col in all_columns])

        return f"""
            INSERT INTO {nds_table} (
                {', '.join([f'{col}_nk' if col in natural_key_columns else col for col in all_columns])},
                source_system,
                natural_key,
                valid_from,
                is_current,
                hash_key,
                nds_loaded_at
            )
            SELECT
                {stg_columns},
                '{source_system}' AS source_system,
                {nk_concat} AS natural_key,
                CURRENT_TIMESTAMP AS valid_from,
                TRUE AS is_current,
                MD5({hash_concat}) AS hash_key,
                CURRENT_TIMESTAMP AS nds_loaded_at
            FROM staging_table stg
            WHERE NOT EXISTS (
                SELECT 1 FROM {nds_table} nds
                WHERE nds.natural_key = {nk_concat}
                  AND nds.is_current = TRUE
            )
        """

    @staticmethod
    def scd_type2_update_expired(
        nds_table: str,
        business_columns: List[str],
        natural_key_expr: str
    ) -> str:
        """
        Generate SQL to expire old records for SCD Type 2.

        Args:
            nds_table: NDS table name
            business_columns: List of business columns for hash comparison
            natural_key_expr: Expression to generate natural key

        Returns:
            UPDATE SQL to expire changed records
        """
        hash_columns = [f"COALESCE(CAST(stg.{col} AS VARCHAR), '')" for col in business_columns]
        hash_concat = ' || \'|\' || '.join(hash_columns)

        return f"""
            UPDATE {nds_table} nds
            SET valid_to = CURRENT_TIMESTAMP,
                is_current = FALSE,
                nds_updated_at = CURRENT_TIMESTAMP
            WHERE nds.is_current = TRUE
              AND EXISTS (
                  SELECT 1 FROM staging_table stg
                  WHERE nds.natural_key = {natural_key_expr}
                    AND nds.hash_key != MD5({hash_concat})
              )
        """

    @staticmethod
    def dimension_upsert(
        dim_table: str,
        business_columns: List[str],
        natural_key_column: str = 'customer_nk'
    ) -> str:
        """
        Generate SQL for dimension table upsert (DDS layer).

        Args:
            dim_table: Dimension table name
            business_columns: List of business columns
            natural_key_column: Natural key column name

        Returns:
            UPSERT SQL statement
        """
        columns_list = ', '.join(business_columns)
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in business_columns])

        return f"""
            INSERT INTO {dim_table} ({columns_list})
            SELECT {columns_list}
            FROM nds_source
            ON CONFLICT ({natural_key_column})
            DO UPDATE SET
                {update_set},
                dds_loaded_at = CURRENT_TIMESTAMP
        """

    @staticmethod
    def fact_incremental_load(
        fact_table: str,
        dim_lookups: dict,
        measure_columns: List[str],
        natural_key_column: str
    ) -> str:
        """
        Generate SQL for fact table incremental load.

        Args:
            fact_table: Fact table name
            dim_lookups: Dictionary mapping dimension names to lookup info
            measure_columns: List of measure column names
            natural_key_column: Natural key column for deduplication

        Returns:
            INSERT SQL with dimension lookups
        """
        # This is a template - actual implementation would be more complex
        return f"""
            INSERT INTO {fact_table} (...)
            SELECT ...
            FROM nds_source
            LEFT JOIN dim_customer ON ...
            LEFT JOIN dim_product ON ...
            WHERE NOT EXISTS (
                SELECT 1 FROM {fact_table} f
                WHERE f.{natural_key_column} = nds_source.{natural_key_column}
            )
        """

    @staticmethod
    def data_quality_check_not_null(
        table_name: str,
        column_name: str
    ) -> str:
        """Generate SQL for NOT NULL quality check."""
        return f"""
            SELECT COUNT(*) AS null_count
            FROM {table_name}
            WHERE {column_name} IS NULL
        """

    @staticmethod
    def data_quality_check_referential_integrity(
        fact_table: str,
        dim_table: str,
        fk_column: str,
        pk_column: str
    ) -> str:
        """Generate SQL for referential integrity check."""
        return f"""
            SELECT COUNT(*) AS orphan_count
            FROM {fact_table} f
            WHERE NOT EXISTS (
                SELECT 1 FROM {dim_table} d
                WHERE d.{pk_column} = f.{fk_column}
            )
        """
