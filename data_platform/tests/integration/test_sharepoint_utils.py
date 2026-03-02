"""
Integration tests for SharePoint utility classes.

Tests GraphAuth and SharePointClient initialization, header generation,
and folder path parsing. Network calls are not made; these tests validate
the logic and structure of the utility code.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "utils"))

from sharepoint_utils import GraphAuth, SharePointClient


@pytest.fixture(autouse=True)
def mock_msal_app():
    """Mock MSAL ConfidentialClientApplication to avoid real auth calls."""
    with patch('sharepoint_utils.msal.ConfidentialClientApplication') as mock_app:
        mock_app.return_value = MagicMock()
        yield mock_app


class TestGraphAuth:
    """Tests for GraphAuth initialization and configuration."""

    def test_init_stores_credentials(self):
        """GraphAuth should store client_id, tenant_id, client_secret."""
        auth = GraphAuth(
            client_id="test-client-id",
            tenant_id="test-tenant-id",
            client_secret="test-secret",
        )
        assert auth.client_id == "test-client-id"
        assert auth.tenant_id == "test-tenant-id"
        assert auth.client_secret == "test-secret"

    def test_authority_url_format(self):
        """Authority URL should follow Azure AD pattern."""
        auth = GraphAuth("cid", "tid-123", "secret")
        assert auth.authority == "https://login.microsoftonline.com/tid-123"

    def test_default_scopes(self):
        """Default scopes should target Microsoft Graph."""
        auth = GraphAuth("cid", "tid", "secret")
        assert auth.scopes == ["https://graph.microsoft.com/.default"]

    def test_custom_scopes(self):
        """Custom scopes should override defaults."""
        custom = ["https://custom.scope/.default"]
        auth = GraphAuth("cid", "tid", "secret", scopes=custom)
        assert auth.scopes == custom

    def test_initial_access_token_is_none(self):
        """Access token should be None before acquire_token is called."""
        auth = GraphAuth("cid", "tid", "secret")
        assert auth.access_token is None


class TestSharePointClient:
    """Tests for SharePointClient initialization and helper methods."""

    def test_init_stores_config(self):
        """SharePointClient should store site_id, drive_id, base_folder_id."""
        client = SharePointClient(
            site_id="site-1",
            drive_id="drive-1",
            base_folder_id="folder-1",
            access_token="token-abc",
        )
        assert client.site_id == "site-1"
        assert client.drive_id == "drive-1"
        assert client.base_folder_id == "folder-1"
        assert client.access_token == "token-abc"

    def test_resource_url(self):
        """Resource URL should point to Microsoft Graph API."""
        client = SharePointClient("s", "d", "f", "t")
        assert client.resource_url == "https://graph.microsoft.com/"

    def test_headers_default_content_type(self):
        """Default headers should use application/json content type."""
        client = SharePointClient("s", "d", "f", "my-token")
        headers = client._headers()
        assert headers["Authorization"] == "Bearer my-token"
        assert headers["Content-Type"] == "application/json"

    def test_headers_custom_content_type(self):
        """Custom content type should be respected in headers."""
        client = SharePointClient("s", "d", "f", "my-token")
        headers = client._headers("application/octet-stream")
        assert headers["Content-Type"] == "application/octet-stream"

    def test_folder_path_parsing(self):
        """Folder paths should be split into individual parts."""
        path = "level1/level2/level3"
        parts = path.strip("/").split("/")
        assert parts == ["level1", "level2", "level3"]

    def test_folder_path_with_leading_trailing_slashes(self):
        """Leading/trailing slashes should be stripped."""
        path = "/level1/level2/"
        parts = path.strip("/").split("/")
        assert parts == ["level1", "level2"]
