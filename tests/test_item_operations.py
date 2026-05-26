# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 2 item operations tests — dual-target against real DynamoDB and extenddb.

Covers: PutItem, GetItem, composite keys, and all DynamoDB attribute types.
REQ-TEST-001, REQ-TEST-002, REQ-TEST-003, REQ-TEST-004
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, scoped_table
@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Create a hash-only table for the class, delete on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name
@pytest.fixture(scope="class")
def hash_range_table(dynamodb_client):
    """Create a hash+range (S,S) table for the class, delete on teardown."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name
@pytest.fixture(scope="class")
def hash_numeric_range_table(dynamodb_client):
    """Create a hash+range (S,N) table for the class, delete on teardown."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name
class TestPutItem:
    """PutItem operation tests."""

    def test_put_and_get_simple_item(self, dynamodb_client, hash_table):
        item = {"pk": {"S": "user-1"}, "name": {"S": "Alice"}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "user-1"}})
        assert resp["Item"] == item

    def test_put_all_attribute_types(self, dynamodb_client, hash_table):
        raw_bytes = b"\x00\x01\x02\xff"
        item = {
            "pk": {"S": "types-test"},
            "str_attr": {"S": "hello"},
            "num_attr": {"N": "42.5"},
            "bin_attr": {"B": raw_bytes},
            "bool_attr": {"BOOL": True},
            "null_attr": {"NULL": True},
            "list_attr": {"L": [{"S": "a"}, {"N": "1"}]},
            "map_attr": {"M": {"nested": {"S": "value"}}},
            "ss_attr": {"SS": ["a", "b", "c"]},
            "ns_attr": {"NS": ["1", "2", "3"]},
            "bs_attr": {"BS": [b"\x01", b"\x02"]},
        }
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "types-test"}})
        got = resp["Item"]

        assert got["pk"] == item["pk"]
        assert got["str_attr"] == item["str_attr"]
        assert got["num_attr"] == item["num_attr"]
        assert got["bin_attr"]["B"] == raw_bytes
        assert got["bool_attr"] == item["bool_attr"]
        assert got["null_attr"] == item["null_attr"]
        assert got["list_attr"] == item["list_attr"]
        assert got["map_attr"] == item["map_attr"]
        # Sets: DynamoDB does not guarantee order, so compare as sets
        assert set(got["ss_attr"]["SS"]) == set(item["ss_attr"]["SS"])
        assert set(got["ns_attr"]["NS"]) == set(item["ns_attr"]["NS"])
        assert set(got["bs_attr"]["BS"]) == {b"\x01", b"\x02"}

    def test_put_return_values_all_old_new_item(self, dynamodb_client, hash_table):
        resp = dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "new-item"}},
            ReturnValues="ALL_OLD",
        )
        assert "Attributes" not in resp

    def test_put_return_values_all_old_overwrite(self, dynamodb_client, hash_table):
        old_item = {"pk": {"S": "overwrite-me"}, "v": {"N": "1"}}
        dynamodb_client.put_item(TableName=hash_table, Item=old_item)
        resp = dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "overwrite-me"}, "v": {"N": "2"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"] == old_item

    def test_put_return_values_none_explicit(self, dynamodb_client, hash_table):
        resp = dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "none-rv"}},
            ReturnValues="NONE",
        )
        assert "Attributes" not in resp

    def test_put_invalid_return_values(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "x"}},
                ReturnValues="ALL_NEW",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_item_exceeds_400kb(self, dynamodb_client, hash_table):
        big_item = {"pk": {"S": "big"}, "data": {"S": "x" * 409_601}}
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(TableName=hash_table, Item=big_item)
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_wrong_key_type(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"N": "123"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_missing_key_attribute(self, dynamodb_client, hash_range_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=hash_range_table,
                Item={"pk": {"S": "only-hash"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName="nonexistent-table-xyz-999",
                Item={"pk": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
class TestGetItem:
    """GetItem operation tests."""

    def test_get_existing_item(self, dynamodb_client, hash_table):
        item = {"pk": {"S": "get-me"}, "data": {"S": "found"}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "get-me"}})
        assert resp["Item"] == item

    def test_get_nonexistent_item(self, dynamodb_client, hash_table):
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "no-such-key"}})
        assert "Item" not in resp

    def test_get_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName="nonexistent-table-xyz-999",
                Key={"pk": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_extra_attributes_in_key(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "x"}, "extra": {"S": "y"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_get_missing_key_attribute(self, dynamodb_client, hash_range_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_range_table,
                Key={"pk": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_get_wrong_key_type(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"N": "123"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"
class TestCompositeKey:
    """Tests for hash+range key tables."""

    def test_put_get_string_range(self, dynamodb_client, hash_range_table):
        item = {"pk": {"S": "user-1"}, "sk": {"S": "profile"}, "data": {"S": "hello"}}
        dynamodb_client.put_item(TableName=hash_range_table, Item=item)
        resp = dynamodb_client.get_item(
            TableName=hash_range_table,
            Key={"pk": {"S": "user-1"}, "sk": {"S": "profile"}},
        )
        assert resp["Item"] == item

    def test_put_get_numeric_range(self, dynamodb_client, hash_numeric_range_table):
        item = {"pk": {"S": "user-1"}, "sk": {"N": "100"}, "data": {"S": "hello"}}
        dynamodb_client.put_item(TableName=hash_numeric_range_table, Item=item)
        resp = dynamodb_client.get_item(
            TableName=hash_numeric_range_table,
            Key={"pk": {"S": "user-1"}, "sk": {"N": "100"}},
        )
        assert resp["Item"] == item

    def test_same_hash_different_range(self, dynamodb_client, hash_range_table):
        item_a = {"pk": {"S": "user-1"}, "sk": {"S": "email"}, "v": {"S": "a"}}
        item_b = {"pk": {"S": "user-1"}, "sk": {"S": "phone"}, "v": {"S": "b"}}
        dynamodb_client.put_item(TableName=hash_range_table, Item=item_a)
        dynamodb_client.put_item(TableName=hash_range_table, Item=item_b)

        resp_a = dynamodb_client.get_item(
            TableName=hash_range_table,
            Key={"pk": {"S": "user-1"}, "sk": {"S": "email"}},
        )
        resp_b = dynamodb_client.get_item(
            TableName=hash_range_table,
            Key={"pk": {"S": "user-1"}, "sk": {"S": "phone"}},
        )
        assert resp_a["Item"] == item_a
        assert resp_b["Item"] == item_b
class TestDataTypes:
    """Edge cases for DynamoDB attribute type fidelity."""

    def test_empty_string_value(self, dynamodb_client, hash_table):
        item = {"pk": {"S": "empty-str"}, "val": {"S": ""}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "empty-str"}})
        assert resp["Item"]["val"] == {"S": ""}

    def test_empty_binary_value(self, dynamodb_client, hash_table):
        item = {"pk": {"S": "empty-bin"}, "val": {"B": b""}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "empty-bin"}})
        assert resp["Item"]["val"]["B"] == b""

    def test_unicode_characters(self, dynamodb_client, hash_table):
        item = {"pk": {"S": "unicode"}, "emoji": {"S": "🎉🚀"}, "cjk": {"S": "日本語"}, "combining": {"S": "é"}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "unicode"}})
        assert resp["Item"]["emoji"] == {"S": "🎉🚀"}
        assert resp["Item"]["cjk"] == {"S": "日本語"}
        assert resp["Item"]["combining"] == {"S": "é"}

    def test_large_number(self, dynamodb_client, hash_table):
        # DynamoDB supports up to 38 significant digits
        big_num = "12345678901234567890123456789012345678"
        item = {"pk": {"S": "big-num"}, "val": {"N": big_num}}
        dynamodb_client.put_item(TableName=hash_table, Item=item)
        resp = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "big-num"}})
        assert resp["Item"]["val"]["N"] == big_num


# ---------------------------------------------------------------------------
# PutItem validation additions (covers commits since 6b98234dcf)
# ---------------------------------------------------------------------------


class TestPutItemValidation:
    """PutItem validation edge cases from recent fixes."""

    @pytest.fixture(scope="class")
    def val_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_put_item_invalid_number_nan(self, dynamodb_client, val_table):
        """PutItem with NaN number returns ValidationException (not SerializationException)."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=val_table,
                Item={"pk": {"S": "x"}, "n": {"N": "NaN"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_item_invalid_number_infinity(self, dynamodb_client, val_table):
        """PutItem with Infinity number returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=val_table,
                Item={"pk": {"S": "x"}, "n": {"N": "Infinity"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_item_number_exceeds_precision(self, dynamodb_client, val_table):
        """PutItem with >38 significant digits returns ValidationException."""
        # 39 significant digits — exceeds DynamoDB's limit.
        big = "1" * 39
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=val_table,
                Item={"pk": {"S": "x"}, "n": {"N": big}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_item_unused_expression_attribute_names(self, dynamodb_client, val_table):
        """Extra ExpressionAttributeNames with ConditionExpression are rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=val_table,
                Item={"pk": {"S": "x"}},
                ConditionExpression="attribute_not_exists(pk)",
                ExpressionAttributeNames={"#unused": "something"},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# UpdateItem tests (covers commits since 6b98234dcf)
# ---------------------------------------------------------------------------


class TestUpdateItem:
    """UpdateItem operation and validation tests."""

    @pytest.fixture(scope="class")
    def upd_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_update_item_set_list_index_beyond_bounds(self, dynamodb_client, upd_table):
        """SET mylist[99] = :v appends to the list when index is beyond bounds."""
        dynamodb_client.put_item(
            TableName=upd_table,
            Item={"pk": {"S": "list-append"}, "mylist": {"L": [{"S": "a"}, {"S": "b"}]}},
        )
        dynamodb_client.update_item(
            TableName=upd_table,
            Key={"pk": {"S": "list-append"}},
            UpdateExpression="SET mylist[99] = :v",
            ExpressionAttributeValues={":v": {"S": "c"}},
        )
        resp = dynamodb_client.get_item(TableName=upd_table, Key={"pk": {"S": "list-append"}})
        items = resp["Item"]["mylist"]["L"]
        assert items[-1] == {"S": "c"}
        assert len(items) == 3

    def test_update_item_missing_intermediate_map_path(self, dynamodb_client, upd_table):
        """SET a.b.c = :v where a.b doesn't exist is rejected."""
        dynamodb_client.put_item(
            TableName=upd_table,
            Item={"pk": {"S": "no-intermediate"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=upd_table,
                Key={"pk": {"S": "no-intermediate"}},
                UpdateExpression="SET #a.#b.#c = :v",
                ExpressionAttributeNames={"#a": "a", "#b": "b", "#c": "c"},
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_update_item_reserved_keyword_without_alias(self, dynamodb_client, upd_table):
        """Using reserved keyword 'status' without #alias is rejected."""
        dynamodb_client.put_item(
            TableName=upd_table,
            Item={"pk": {"S": "reserved-kw"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=upd_table,
                Key={"pk": {"S": "reserved-kw"}},
                UpdateExpression="SET status = :v",
                ExpressionAttributeValues={":v": {"S": "active"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_update_item_empty_update_expression(self, dynamodb_client, upd_table):
        """Empty UpdateExpression string is rejected."""
        dynamodb_client.put_item(
            TableName=upd_table,
            Item={"pk": {"S": "empty-expr"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=upd_table,
                Key={"pk": {"S": "empty-expr"}},
                UpdateExpression="",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_update_item_condition_on_nonexistent_item(self, dynamodb_client, upd_table):
        """attribute_not_exists on a missing item succeeds (creates the item)."""
        dynamodb_client.update_item(
            TableName=upd_table,
            Key={"pk": {"S": "cond-new-item"}},
            UpdateExpression="SET #d = :v",
            ConditionExpression="attribute_not_exists(pk)",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":v": {"S": "created"}},
        )
        resp = dynamodb_client.get_item(TableName=upd_table, Key={"pk": {"S": "cond-new-item"}})
        assert resp["Item"]["data"]["S"] == "created"

    def test_update_item_ne_comparison_missing_attribute(self, dynamodb_client, upd_table):
        """ConditionExpression 'attr <> :val' passes when attr is absent."""
        dynamodb_client.put_item(
            TableName=upd_table,
            Item={"pk": {"S": "ne-missing"}},
        )
        # attr doesn't exist on the item — <> should evaluate to true.
        dynamodb_client.update_item(
            TableName=upd_table,
            Key={"pk": {"S": "ne-missing"}},
            UpdateExpression="SET #d = :v",
            ConditionExpression="#a <> :cmp",
            ExpressionAttributeNames={"#d": "data", "#a": "nonexistent_attr"},
            ExpressionAttributeValues={":v": {"S": "ok"}, ":cmp": {"S": "anything"}},
        )
        resp = dynamodb_client.get_item(TableName=upd_table, Key={"pk": {"S": "ne-missing"}})
        assert resp["Item"]["data"]["S"] == "ok"


# ---------------------------------------------------------------------------
# DeleteItem tests
# ---------------------------------------------------------------------------


class TestDeleteItem:
    """DeleteItem operation tests."""

    @pytest.fixture(scope="class")
    def del_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def test_delete_item_nonexistent_table(self, dynamodb_client):
        """DeleteItem on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName="nonexistent-table-xyz-999",
                Key={"pk": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_item_condition_on_nonexistent_item(self, dynamodb_client, del_table):
        """DeleteItem with attribute_not_exists on missing item succeeds (no-op)."""
        # Item doesn't exist — condition passes, delete is a no-op.
        dynamodb_client.delete_item(
            TableName=del_table,
            Key={"pk": {"S": "ghost-delete"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
        # Verify no item was created.
        resp = dynamodb_client.get_item(TableName=del_table, Key={"pk": {"S": "ghost-delete"}})
        assert "Item" not in resp


# ---------------------------------------------------------------------------
# Projection tests
# ---------------------------------------------------------------------------


class TestProjection:
    """ProjectionExpression edge cases."""

    @pytest.fixture(scope="class")
    def proj_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": "proj-1"},
                    "mylist": {"L": [{"S": "zero"}, {"S": "one"}, {"S": "two"}]},
                    "nested": {"M": {"inner": {"S": "deep"}}},
                },
            )
            yield name

    def test_projection_list_index(self, dynamodb_client, proj_table):
        """ProjectionExpression with list index returns element wrapped in a list."""
        resp = dynamodb_client.get_item(
            TableName=proj_table,
            Key={"pk": {"S": "proj-1"}},
            ProjectionExpression="mylist[1]",
        )
        item = resp["Item"]
        # DynamoDB returns the projected element inside a single-element list.
        assert "mylist" in item
        assert item["mylist"]["L"] == [{"S": "one"}]

    def test_projection_list_index_out_of_bounds(self, dynamodb_client, proj_table):
        """ProjectionExpression with out-of-bounds list index returns empty item."""
        resp = dynamodb_client.get_item(
            TableName=proj_table,
            Key={"pk": {"S": "proj-1"}},
            ProjectionExpression="mylist[99]",
        )
        # Out-of-bounds index — attribute not included in response.
        assert "mylist" not in resp.get("Item", {})
