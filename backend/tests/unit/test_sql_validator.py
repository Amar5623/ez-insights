"""
Dev 2 owns this file.
Tests for sql_validator — no DB needed, pure logic.
Run: pytest tests/unit/test_sql_validator.py -v
"""
import pytest
from strategies.sql_validator import validate_sql


def test_valid_select_passes():
    sql = "SELECT id, name FROM products WHERE price > 20"
    valid, error = validate_sql(sql)
    assert valid is True
    assert error is None


def test_drop_table_is_blocked():
    sql = "DROP TABLE users"
    valid, error = validate_sql(sql)
    assert valid is False
    assert error is not None


def test_delete_is_blocked():
    sql = "DELETE FROM orders WHERE id = 1"
    valid, error = validate_sql(sql)
    assert valid is False


def test_truncate_is_blocked():
    valid, error = validate_sql("TRUNCATE TABLE logs")
    assert valid is False


def test_stacked_statements_are_blocked():
    sql = "SELECT * FROM users; DROP TABLE users"
    valid, error = validate_sql(sql)
    assert valid is False


def test_update_is_blocked():
    valid, error = validate_sql("UPDATE products SET price = 0")
    assert valid is False


def test_insert_is_blocked():
    valid, error = validate_sql("INSERT INTO users VALUES (1, 'hack')")
    assert valid is False


def test_select_with_join_passes():
    sql = """
    SELECT p.name, c.name as category
    FROM products p
    JOIN categories c ON p.category_id = c.id
    WHERE p.price < 50
    """
    valid, error = validate_sql(sql)
    assert valid is True
