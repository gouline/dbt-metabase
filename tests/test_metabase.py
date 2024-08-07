import datetime
import json
from unittest import mock

import pytest

from tests._mocks import MockMetabase


@pytest.fixture(name="metabase")
def fixture_metabase() -> MockMetabase:
    return MockMetabase(url="http://localhost")


def test_metabase_find_database(metabase: MockMetabase):
    db = metabase.find_database(name="dbtmetabase")
    assert db
    assert db["id"] == 2
    assert metabase.find_database(name="foo") is None


def test_metabase_get_collections(metabase: MockMetabase):
    excluded = metabase.get_collections(exclude_personal=True)
    assert len(excluded) == 1

    included = metabase.get_collections(exclude_personal=False)
    assert len(included) == 2


def test_metabase_get_collection_items(metabase: MockMetabase):
    cards = metabase.get_collection_items(
        uid="root",
        models=("card",),
    )
    assert {item["model"] for item in cards} == {"card"}

    dashboards = metabase.get_collection_items(
        uid="root",
        models=("dashboard",),
    )
    assert {item["model"] for item in dashboards} == {"dashboard"}

    both = metabase.get_collection_items(
        uid="root",
        models=("card", "dashboard"),
    )
    assert {item["model"] for item in both} == {"card", "dashboard"}


@pytest.fixture(name="metabase_gcp_iap")
def fixture_metabase_gcp_iap() -> MockMetabase:
    return MockMetabase(
        gcp_iap_service_account="service-account@your-project.iam.gserviceaccount.com",
        url="https://example.com",
    )


@mock.patch("google.auth.default")
@mock.patch("google.cloud.iam_credentials_v1.IAMCredentialsClient")
@mock.patch("datetime.datetime")
def test_generate_gcp_iap_token(
    mock_datetime,
    mock_iam_credentials_client,
    mock_google_auth_default,
    metabase_gcp_iap,
):
    # Mock the current time
    mock_now = datetime.datetime(2024, 8, 7, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    # Convert datetime to Unix timestamp for validation
    mock_now_timestamp = int(mock_now.timestamp())

    # Mock the credentials and the sign_jwt method
    mock_source_credentials = mock.Mock()
    mock_google_auth_default.return_value = (mock_source_credentials, None)

    mock_iam_client_instance = mock.Mock()
    mock_iam_credentials_client.return_value = mock_iam_client_instance

    mock_signed_jwt = "mocked_signed_jwt_token"
    mock_iam_client_instance.sign_jwt.return_value.signed_jwt = mock_signed_jwt

    # Mock service_account_path to return a mock value
    mock_iam_client_instance.service_account_path.return_value = (
        "mocked_service_account_path"
    )

    # Call the method under test
    token = metabase_gcp_iap._generate_gcp_iap_token("/test-endpoint")

    # Verify that the correct JWT payload is being created
    expected_payload = json.dumps(
        {
            "iss": "service-account@your-project.iam.gserviceaccount.com",
            "sub": "service-account@your-project.iam.gserviceaccount.com",
            "aud": "https://example.com/test-endpoint",
            "iat": mock_now_timestamp,  # Expected Unix timestamp
            "exp": mock_now_timestamp + 10,  # Expected Unix timestamp + 10 seconds
        }
    )

    mock_iam_client_instance.sign_jwt.assert_called_once_with(
        name="mocked_service_account_path", payload=expected_payload
    )

    # Verify the returned token
    assert token == mock_signed_jwt
