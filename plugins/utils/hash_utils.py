"""
Utility functions for hash-based change detection in SCD Type 2.
"""

import hashlib
from typing import Any, Dict, List, Optional


def generate_hash_key(data: Dict[str, Any], columns: List[str]) -> str:
    """
    Generate MD5 hash for change detection.

    Args:
        data: Dictionary containing row data
        columns: List of column names to include in hash

    Returns:
        MD5 hash as hexadecimal string
    """
    # Sort columns to ensure consistent ordering
    sorted_columns = sorted(columns)

    # Extract values in sorted order, converting None to empty string
    values = [str(data.get(col, '')) for col in sorted_columns]

    # Concatenate values and generate hash
    concat_string = '|'.join(values)
    return hashlib.md5(concat_string.encode('utf-8')).hexdigest()


def generate_natural_key(data: Dict[str, Any], key_columns: List[str], separator: str = '-') -> str:
    """
    Generate natural key by concatenating specified columns.

    Args:
        data: Dictionary containing row data
        key_columns: List of columns to use for natural key
        separator: Separator character between key components

    Returns:
        Natural key string
    """
    key_values = [str(data.get(col, '')) for col in key_columns]
    return separator.join(key_values)


def compare_rows(row1: Dict[str, Any], row2: Dict[str, Any], columns_to_compare: List[str]) -> bool:
    """
    Compare two rows based on specified columns.

    Args:
        row1: First row dictionary
        row2: Second row dictionary
        columns_to_compare: List of column names to compare

    Returns:
        True if rows are identical, False otherwise
    """
    for col in columns_to_compare:
        if row1.get(col) != row2.get(col):
            return False
    return True


def get_business_columns(all_columns: List[str], exclude_patterns: Optional[List[str]] = None) -> List[str]:
    """
    Filter business columns by excluding audit/technical columns.

    Args:
        all_columns: List of all column names
        exclude_patterns: List of patterns to exclude (e.g., ['created_at', 'updated_at', 'loaded_at'])

    Returns:
        List of business column names
    """
    if exclude_patterns is None:
        exclude_patterns = [
            'created_at', 'updated_at', 'loaded_at',
            'stg_', 'nds_', 'dds_',
            '_sk', '_nk', 'valid_from', 'valid_to',
            'is_current', 'hash_key', 'source_system'
        ]

    business_columns = []
    for col in all_columns:
        should_exclude = False
        for pattern in exclude_patterns:
            if pattern in col.lower():
                should_exclude = True
                break
        if not should_exclude:
            business_columns.append(col)

    return business_columns
