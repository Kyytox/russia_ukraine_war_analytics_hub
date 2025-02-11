import pytest
import pandas as pd

from core.process_data_warehouse.ingest.ingest_ru_block_sites import (
    format_values,
)

from core.config.dwh_corresp_schema import CORRESP_SCHEMA_RUSSIA_BLOCK_SITES

from core.libs.utils import rename_cols


@pytest.fixture
def valid_df():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame(
        {
            "Domain": ["signal.org", "facebook.com", "discord.com"],
            "Banning Authority": [
                "Not specified",
                "Office of the Prosecutor General",
                "Roskomnadzor",
            ],
            "Locale": ["United States", "France", "Germany"],
            "Type of site": ["Messaging", "Social Media", "Social Media"],
            "Subcategory": ["", "-", "Ukraine Support Content"],
            "Date blocked": ["8/8/2021", "9/8/2021", "10/8/2021"],
        }
    )


class TestFormatValues:
    def test_valid_format(self, valid_df):

        # rename Subcategory to subcategory, date blocked to date_blocked
        valid_df = valid_df.rename(
            columns={
                "Subcategory": "subcategory",
                "Date blocked": "date_blocked",
            }
        )

        result = format_values(valid_df)

        # checkl if no '' and '-' in subcategory
        assert not any(result["subcategory"].isin(["", "-"]))

        # check if date_blocked is datetime
        assert result["date_blocked"].dtype == "datetime64[ns]"


class TestRenameColumns:
    def test_valid_rename(self, valid_df):
        result = rename_cols(valid_df, CORRESP_SCHEMA_RUSSIA_BLOCK_SITES)

        # check if the columns are renamed
        assert result.columns.tolist() == list(
            CORRESP_SCHEMA_RUSSIA_BLOCK_SITES.values()
        )

        # check if the columns are renamed correctly
        assert all(
            [
                col in result.columns
                for col in CORRESP_SCHEMA_RUSSIA_BLOCK_SITES.values()
            ]
        )
