"""
tests/unit/test_sql_validator.py
Dev 2 owns this file.

Tests for sql_validator — no DB needed, pure logic.
Run: pytest tests/unit/test_sql_validator.py -v

Coverage:
    MySQLValidator
        ✓ Valid SELECT queries pass
        ✓ Dangerous DDL/DML keywords blocked
        ✓ Stacked statement injection blocked
        ✓ Comment injection blocked
        ✓ UNION injection blocked
        ✓ Tautology injection blocked
        ✓ Dangerous function calls blocked
        ✓ Hex / CHAR encoding tricks blocked
        ✓ Subquery exfiltration blocked
        ✓ Non-string input rejected
        ✓ Empty query rejected
        ✓ Must start with SELECT

    MongoValidator
        ✓ Valid filter dicts pass
        ✓ $where (JS execution) blocked
        ✓ $function (JS execution) blocked
        ✓ $accumulator (JS execution) blocked
        ✓ Write operators blocked ($set, $push, etc.)
        ✓ Aggregation output operators blocked ($out, $merge)
        ✓ Unanchored $regex blocked
        ✓ Anchored $regex allowed
        ✓ Deep nesting guard
        ✓ Nested dangerous operator blocked (inside nested dict)
        ✓ Non-dict/list input rejected
        ✓ Empty pipeline allowed
        ✓ Valid aggregation pipeline passes

    get_validator factory
        ✓ Returns MySQLValidator for "mysql"
        ✓ Returns MongoValidator for "mongo" / "mongodb"
        ✓ Raises ValueError for unknown db_type

    validate_sql backward compatibility
        ✓ Still works via legacy wrapper
"""

import pytest
from strategies.sql_validator import (
    MySQLValidator,
    MongoValidator,
    get_validator,
    validate_sql,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def mysql():
    return MySQLValidator()


@pytest.fixture
def mongo():
    return MongoValidator()


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — valid queries
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorValidQueries:

    def test_simple_select_passes(self, mysql):
        valid, error = mysql.validate("SELECT id, name FROM products WHERE price > 20")
        assert valid is True
        assert error is None

    def test_select_with_join_passes(self, mysql):
        sql = """
        SELECT p.name, c.name AS category
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE p.price < 50
        """
        valid, error = mysql.validate(sql)
        assert valid is True
        assert error is None

    def test_select_with_order_by_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT name, price FROM books ORDER BY price ASC LIMIT 10"
        )
        assert valid is True

    def test_select_with_group_by_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5"
        )
        assert valid is True

    def test_select_with_like_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT * FROM users WHERE name LIKE 'John%'"
        )
        assert valid is True

    def test_select_with_between_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'"
        )
        assert valid is True

    def test_select_with_in_clause_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT * FROM products WHERE category IN ('Books', 'Music')"
        )
        assert valid is True

    def test_select_with_trailing_semicolon_passes(self, mysql):
        # A single trailing semicolon is acceptable (common in copy-pasted SQL)
        valid, error = mysql.validate("SELECT * FROM products;")
        assert valid is True

    def test_select_with_is_null_passes(self, mysql):
        valid, error = mysql.validate(
            "SELECT * FROM orders WHERE shipped_at IS NULL"
        )
        assert valid is True


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — must start with SELECT
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorMustBeSelect:

    def test_drop_table_blocked(self, mysql):
        valid, error = mysql.validate("DROP TABLE users")
        assert valid is False
        assert error is not None

    def test_delete_blocked(self, mysql):
        valid, error = mysql.validate("DELETE FROM orders WHERE id = 1")
        assert valid is False

    def test_truncate_blocked(self, mysql):
        valid, error = mysql.validate("TRUNCATE TABLE logs")
        assert valid is False

    def test_insert_blocked(self, mysql):
        valid, error = mysql.validate("INSERT INTO users VALUES (1, 'hack')")
        assert valid is False

    def test_update_blocked(self, mysql):
        valid, error = mysql.validate("UPDATE products SET price = 0")
        assert valid is False

    def test_create_table_blocked(self, mysql):
        valid, error = mysql.validate("CREATE TABLE pwned (id INT)")
        assert valid is False

    def test_alter_table_blocked(self, mysql):
        valid, error = mysql.validate("ALTER TABLE users ADD COLUMN pw TEXT")
        assert valid is False

    def test_grant_blocked(self, mysql):
        valid, error = mysql.validate("GRANT ALL ON *.* TO 'hacker'@'%'")
        assert valid is False

    def test_revoke_blocked(self, mysql):
        valid, error = mysql.validate("REVOKE SELECT ON db.* FROM 'user'@'host'")
        assert valid is False

    def test_exec_blocked(self, mysql):
        valid, error = mysql.validate("EXEC xp_cmdshell('dir')")
        assert valid is False

    def test_call_blocked(self, mysql):
        valid, error = mysql.validate("CALL malicious_proc()")
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — stacked statement injection
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorStackedStatements:

    def test_stacked_drop_blocked(self, mysql):
        sql = "SELECT * FROM users; DROP TABLE users"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_stacked_update_blocked(self, mysql):
        sql = "SELECT id FROM users; UPDATE users SET password='hacked'"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_stacked_insert_blocked(self, mysql):
        sql = "SELECT 1; INSERT INTO admins VALUES (99, 'attacker')"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_stacked_semicolons_blocked(self, mysql):
        sql = "SELECT 1;; DROP TABLE users"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — comment injection
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorCommentInjection:

    def test_double_dash_comment_blocked(self, mysql):
        # Classic: ' OR '1'='1' --
        sql = "SELECT * FROM users WHERE username='admin' -- AND password='x'"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_hash_comment_blocked(self, mysql):
        sql = "SELECT * FROM users WHERE id=1 # OR 1=1"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_block_comment_blocked(self, mysql):
        sql = "SELECT * FROM users /* malicious comment */"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_inline_block_comment_bypass_attempt_blocked(self, mysql):
        # Attacker may try: SEL/**/ECT — sqlparse handles this,
        # the comment regex catches the /* regardless.
        sql = "SEL/**/ECT * FROM users"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — UNION injection
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorUnionInjection:

    def test_union_select_blocked(self, mysql):
        sql = "SELECT id FROM products UNION SELECT password FROM users"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_union_all_select_blocked(self, mysql):
        sql = "SELECT id FROM products UNION ALL SELECT username FROM admins"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_union_with_null_exfil_blocked(self, mysql):
        # Attacker probes column count: UNION SELECT NULL,NULL,NULL
        sql = "SELECT id FROM products UNION SELECT NULL,NULL,NULL"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — tautology injection
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorTautologyInjection:

    def test_or_1_eq_1_blocked(self, mysql):
        sql = "SELECT * FROM users WHERE id=1 OR 1=1"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_or_true_blocked(self, mysql):
        sql = "SELECT * FROM users WHERE id=1 OR TRUE"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_or_string_eq_string_blocked(self, mysql):
        sql = "SELECT * FROM users WHERE id=1 OR 'a'='a'"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — dangerous functions
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorDangerousFunctions:

    def test_sleep_blocked(self, mysql):
        # Time-based blind injection
        sql = "SELECT * FROM users WHERE id=1 AND SLEEP(5)"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_benchmark_blocked(self, mysql):
        # CPU-based blind injection
        sql = "SELECT BENCHMARK(1000000, MD5('a'))"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_load_file_blocked(self, mysql):
        # Read arbitrary server files
        sql = "SELECT LOAD_FILE('/etc/passwd')"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_version_function_blocked(self, mysql):
        # Information leakage
        sql = "SELECT VERSION()"
        valid, error = mysql.validate(sql)
        assert valid is False

    def test_database_function_blocked(self, mysql):
        sql = "SELECT DATABASE()"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — hex / encoding tricks
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorEncodingTricks:

    def test_hex_literal_blocked(self, mysql):
        # Attacker encodes 'DROP' as hex: 0x44524f50
        sql = "SELECT 0x44524f50"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_char_function_blocked(self, mysql):
        # CHAR(68,82,79,80) = 'DROP'
        sql = "SELECT CHAR(68,82,79,80)"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — subquery exfiltration
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorSubqueryExfiltration:

    def test_nested_select_blocked(self, mysql):
        sql = "SELECT (SELECT password FROM users LIMIT 1)"
        valid, error = mysql.validate(sql)
        assert valid is False
        assert error is not None

    def test_nested_select_in_where_blocked(self, mysql):
        sql = "SELECT id FROM products WHERE id = (SELECT MAX(id) FROM users)"
        valid, error = mysql.validate(sql)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MySQLValidator — input validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestMySQLValidatorInputValidation:

    def test_non_string_dict_rejected(self, mysql):
        valid, error = mysql.validate({"filter": "price > 10"})
        assert valid is False
        assert "string" in error.lower()

    def test_non_string_int_rejected(self, mysql):
        valid, error = mysql.validate(42)
        assert valid is False

    def test_empty_string_rejected(self, mysql):
        valid, error = mysql.validate("")
        assert valid is False
        assert error is not None

    def test_whitespace_only_rejected(self, mysql):
        valid, error = mysql.validate("   \n\t  ")
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — valid queries
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorValidQueries:

    def test_simple_filter_passes(self, mongo):
        valid, error = mongo.validate({"age": {"$gt": 20}})
        assert valid is True
        assert error is None

    def test_empty_filter_passes(self, mongo):
        # {} means "return all documents"
        valid, error = mongo.validate({})
        assert valid is True

    def test_string_equality_filter_passes(self, mongo):
        valid, error = mongo.validate({"status": "active"})
        assert valid is True

    def test_nested_comparison_passes(self, mongo):
        valid, error = mongo.validate({
            "price": {"$gte": 10, "$lte": 50},
            "in_stock": True,
        })
        assert valid is True

    def test_in_operator_passes(self, mongo):
        valid, error = mongo.validate({"category": {"$in": ["Books", "Music"]}})
        assert valid is True

    def test_anchored_regex_passes(self, mongo):
        # Anchored regex is safe — won't cause ReDoS
        valid, error = mongo.validate({"name": {"$regex": "^John"}})
        assert valid is True

    def test_empty_pipeline_passes(self, mongo):
        # Empty aggregation pipeline is valid
        valid, error = mongo.validate([])
        assert valid is True

    def test_valid_aggregation_pipeline_passes(self, mongo):
        pipeline = [
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$category", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
        ]
        valid, error = mongo.validate(pipeline)
        assert valid is True

    def test_and_or_operators_pass(self, mongo):
        valid, error = mongo.validate({
            "$or": [
                {"price": {"$lt": 20}},
                {"in_stock": True},
            ]
        })
        assert valid is True


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — JavaScript execution operators
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorJSExecution:

    def test_where_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$where": "this.age > 20"})
        assert valid is False
        assert "$where" in error

    def test_where_with_function_string_blocked(self, mongo):
        valid, error = mongo.validate({"$where": "function() { return true; }"})
        assert valid is False

    def test_function_operator_blocked(self, mongo):
        valid, error = mongo.validate({
            "$function": {
                "body": "function(x) { return x; }",
                "args": ["$price"],
                "lang": "js",
            }
        })
        assert valid is False
        assert "$function" in error

    def test_accumulator_operator_blocked(self, mongo):
        valid, error = mongo.validate({
            "$accumulator": {
                "init": "function() { return 0; }",
                "accumulate": "function(state, x) { return state + x; }",
                "accumulateArgs": ["$price"],
                "merge": "function(s1, s2) { return s1 + s2; }",
                "lang": "js",
            }
        })
        assert valid is False
        assert "$accumulator" in error

    def test_nested_where_blocked(self, mongo):
        # Dangerous operator hidden inside a nested dict
        valid, error = mongo.validate({
            "user": {
                "$where": "this.admin === true"
            }
        })
        assert valid is False

    def test_where_in_pipeline_stage_blocked(self, mongo):
        pipeline = [
            {"$match": {"$where": "this.price > 10"}}
        ]
        valid, error = mongo.validate(pipeline)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — write operators in read query
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorWriteOperators:

    def test_set_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$set": {"price": 0}})
        assert valid is False
        assert "$set" in error

    def test_unset_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$unset": {"password": ""}})
        assert valid is False

    def test_push_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$push": {"roles": "admin"}})
        assert valid is False

    def test_pull_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$pull": {"roles": "user"}})
        assert valid is False

    def test_inc_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$inc": {"balance": 1000000}})
        assert valid is False

    def test_rename_operator_blocked(self, mongo):
        valid, error = mongo.validate({"$rename": {"old_field": "new_field"}})
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — aggregation output operators
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorAggregationOutputOperators:

    def test_out_operator_blocked(self, mongo):
        pipeline = [
            {"$match": {"status": "active"}},
            {"$out": "stolen_data"},
        ]
        valid, error = mongo.validate(pipeline)
        assert valid is False
        assert "$out" in error

    def test_merge_operator_blocked(self, mongo):
        pipeline = [
            {"$group": {"_id": "$category"}},
            {"$merge": {"into": "target_collection"}},
        ]
        valid, error = mongo.validate(pipeline)
        assert valid is False
        assert "$merge" in error


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — regex DoS
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorRegexDoS:

    def test_unanchored_regex_blocked(self, mongo):
        valid, error = mongo.validate({"name": {"$regex": ".*evil.*"}})
        assert valid is False
        assert "anchor" in error.lower() or "regex" in error.lower()

    def test_regex_without_anchor_blocked(self, mongo):
        valid, error = mongo.validate({"email": {"$regex": "gmail.com"}})
        assert valid is False

    def test_anchored_regex_passes(self, mongo):
        valid, error = mongo.validate({"username": {"$regex": "^admin"}})
        assert valid is True


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — deep nesting guard
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorDeepNesting:

    def test_excessive_nesting_blocked(self, mongo):
        # Build a dict nested 15 levels deep — exceeds _MONGO_MAX_DEPTH=10
        doc: dict = {}
        cursor = doc
        for _ in range(15):
            cursor["nested"] = {}
            cursor = cursor["nested"]
        cursor["value"] = 1

        valid, error = mongo.validate(doc)
        assert valid is False
        assert "depth" in error.lower() or "nest" in error.lower()


# ═══════════════════════════════════════════════════════════════════════════════
# MongoValidator — input validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestMongoValidatorInputValidation:

    def test_string_input_rejected(self, mongo):
        valid, error = mongo.validate("db.users.find({})")
        assert valid is False
        assert "dict" in error.lower() or "list" in error.lower()

    def test_integer_input_rejected(self, mongo):
        valid, error = mongo.validate(42)
        assert valid is False

    def test_none_input_rejected(self, mongo):
        valid, error = mongo.validate(None)
        assert valid is False


# ═══════════════════════════════════════════════════════════════════════════════
# get_validator factory
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetValidatorFactory:

    def test_returns_mysql_validator_for_mysql(self):
        v = get_validator("mysql")
        assert isinstance(v, MySQLValidator)

    def test_returns_mongo_validator_for_mongo(self):
        v = get_validator("mongo")
        assert isinstance(v, MongoValidator)

    def test_returns_mongo_validator_for_mongodb(self):
        v = get_validator("mongodb")
        assert isinstance(v, MongoValidator)

    def test_case_insensitive_mysql(self):
        v = get_validator("MySQL")
        assert isinstance(v, MySQLValidator)

    def test_case_insensitive_mongo(self):
        v = get_validator("MONGO")
        assert isinstance(v, MongoValidator)

    def test_raises_for_unknown_db_type(self):
        with pytest.raises(ValueError, match="Unknown db_type"):
            get_validator("postgres")

    def test_raises_for_empty_string(self):
        with pytest.raises(ValueError):
            get_validator("")


# ═══════════════════════════════════════════════════════════════════════════════
# validate_sql backward-compatibility wrapper
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateSqlBackwardCompatibility:

    def test_valid_select_passes(self):
        valid, error = validate_sql("SELECT id, name FROM products WHERE price > 20")
        assert valid is True
        assert error is None

    def test_drop_blocked(self):
        valid, error = validate_sql("DROP TABLE users")
        assert valid is False

    def test_stacked_statement_blocked(self):
        valid, error = validate_sql("SELECT * FROM users; DROP TABLE users")
        assert valid is False

    def test_returns_tuple(self):
        result = validate_sql("SELECT 1")
        assert isinstance(result, tuple)
        assert len(result) == 2