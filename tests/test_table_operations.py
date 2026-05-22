# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 1 table operations tests — dual-target against real DynamoDB and extenddb.

Covers: CreateTable, DescribeTable, ListTables, DeleteTable, and key error paths.
REQ-TEST-001, REQ-TEST-002, REQ-TEST-003, REQ-TEST-004
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import wait_for_active
class TestCreateTable:
    """CreateTable operation tests."""

    def test_create_simple_hash_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(unique_table_name)
        desc = result["TableDescription"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("ACTIVE", "CREATING")
        assert len(desc["KeySchema"]) == 1
        assert desc["KeySchema"][0]["KeyType"] == "HASH"
        assert desc["TableArn"].endswith(f"table/{unique_table_name}")

    def test_create_hash_range_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(
            unique_table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        )
        desc = result["TableDescription"]
        assert len(desc["KeySchema"]) == 2
        assert desc["KeySchema"][1]["KeyType"] == "RANGE"

    def test_create_provisioned_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        result = create_and_cleanup_table(
            unique_table_name,
            BillingMode="PROVISIONED",
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        desc = result["TableDescription"]
        pt = desc["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 5
        assert pt["WriteCapacityUnits"] == 5

    def test_create_duplicate_table_fails(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            create_and_cleanup_table(unique_table_name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_create_table_name_too_short(self, dynamodb_client):
        from botocore.exceptions import ClientError, ParamValidationError

        with pytest.raises((ParamValidationError, ClientError)):
            dynamodb_client.create_table(
                TableName="ab",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )

    def test_create_table_invalid_characters(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.create_table(
                TableName="invalid table!",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
class TestDescribeTable:
    """DescribeTable operation tests."""

    def test_describe_existing_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        result = dynamodb_client.describe_table(TableName=unique_table_name)
        desc = result["Table"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("ACTIVE", "CREATING")
        assert "TableArn" in desc
        assert "TableId" in desc
        assert "CreationDateTime" in desc
        assert "TableSizeBytes" in desc
        assert "ItemCount" in desc

    def test_describe_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.describe_table(TableName="nonexistent-table-xyz-999")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
class TestListTables:
    """ListTables operation tests."""

    def test_list_tables_returns_created_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        # Paginate through all tables — the default page size is 100 and
        # the test database may contain more tables than that.
        collected: list[str] = []
        kwargs: dict = {}
        while True:
            result = dynamodb_client.list_tables(**kwargs)
            collected.extend(result["TableNames"])
            if "LastEvaluatedTableName" not in result:
                break
            kwargs["ExclusiveStartTableName"] = result["LastEvaluatedTableName"]
        assert unique_table_name in collected

    def test_list_tables_with_limit(self, create_and_cleanup_table, dynamodb_client):
        result = dynamodb_client.list_tables(Limit=1)
        assert len(result["TableNames"]) <= 1

    def test_list_tables_pagination(self, create_and_cleanup_table, dynamodb_client):
        # Create 3 tables, paginate with limit=1
        names = []
        for _ in range(3):
            name = f"extenddb-test-page-{uuid.uuid4().hex[:8]}"
            create_and_cleanup_table(name)
            names.append(name)

        collected = []
        kwargs: dict = {"Limit": 1}
        while True:
            result = dynamodb_client.list_tables(**kwargs)
            collected.extend(result["TableNames"])
            if "LastEvaluatedTableName" not in result:
                break
            kwargs["ExclusiveStartTableName"] = result["LastEvaluatedTableName"]

        # All 3 created tables should appear in the full listing
        for name in names:
            assert name in collected
class TestDeleteTable:
    """DeleteTable operation tests."""

    def test_delete_existing_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(unique_table_name)
        result = dynamodb_client.delete_table(TableName=unique_table_name)
        desc = result["TableDescription"]
        assert desc["TableName"] == unique_table_name
        assert desc["TableStatus"] in ("DELETING", "ACTIVE")

    def test_delete_nonexistent_table(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_table(TableName="nonexistent-table-xyz-999")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"

    def test_delete_protected_table(self, create_and_cleanup_table, dynamodb_client, unique_table_name):
        create_and_cleanup_table(
            unique_table_name,
            DeletionProtectionEnabled=True,
        )
        wait_for_active(dynamodb_client, unique_table_name)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_table(TableName=unique_table_name)
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "deletion protection" in err["Message"].lower() or "protected" in err["Message"].lower()

        # Cleanup: disable protection so fixture can delete
        dynamodb_client.update_table(
            TableName=unique_table_name,
            DeletionProtectionEnabled=False,
        )
