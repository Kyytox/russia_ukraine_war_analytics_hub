import pytest
import pandas as pd

from core.process_data_warehouse.ingest.ingest_inc_railway import (
    ctrl_date,
    ctrl_regions,
    ctrl_miss_val_solo_col,
    ctrl_miss_val_set_cols,
    ctrl_missing_values_prtsn,
    ctrl_cols,
)

from core.config.dwh_corresp_schema import CORRESP_SCHEMA_INCIDENT_RAILWAY

from core.libs.utils import get_regions_geojson, rename_cols


@pytest.fixture
def valid_df():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame(
        {
            "IDX": ["rail1", "rail2", "rail3"],
            "date": ["1/1/2023", "2/2/2023", "3/3/2023"],
            "region": ["Moscow", "Belgorod", "Chelyabinsk"],
            "dmg_eqp": ["Equipment1", "Equipment2", "Equipment3"],
            "inc_type": ["Sabotage", "Collision", "Sabotage"],
            "source_links": ["link1", "link2", "link3"],
            "prtsn_grp": ["Group1", "Group2", "Group3"],
            "coll_with": ["Object1", "Object2", "Object3"],
        }
    )


@pytest.fixture
def error_df():
    """Create a sample DataFrame with errors for testing"""
    return pd.DataFrame(
        {
            "IDX": ["rail1", "rail2", "rail3"],
            "date": ["1/1/2023", None, "3-3-2023"],
            "region": ["Moscow", "St Petersburg", "Vladivostok"],
            "dmg_eqp": ["Equipment1", None, "Equipment3"],
            "inc_type": ["Sabotage", None, "collision"],
            "source_links": ["link1", "link2", None],
            "prtsn_grp": [None, "Group2", "Group3"],
            "coll_with": ["Object1", "Object2", None],
        }
    )


@pytest.fixture
def dict_region():
    """get region from json file"""
    return get_regions_geojson()


class TestCtrlDate:
    def test_valid_dates(self, valid_df):
        result = ctrl_date(valid_df)
        assert result.is_completed()

    def test_invalid_dates(self, error_df):
        result = ctrl_date(error_df)
        assert result.is_failed()


class TestCtrlRegions:
    def test_valid_regions(self, valid_df, dict_region):
        result = ctrl_regions(valid_df, dict_region)
        assert result.is_completed()

    def test_invalid_regions(self, error_df, dict_region):
        result = ctrl_regions(error_df, dict_region)
        assert result.is_failed()


class TestCtrlMissValuesCol:
    def test_no_missing_values(self, valid_df):
        result = ctrl_miss_val_solo_col(valid_df)
        assert result.is_completed()

    def test_missing_values(self, error_df):
        result = ctrl_miss_val_solo_col(error_df)
        assert result.is_failed()


class TestCtrlMissValuesTypInc:
    def test_no_missing_values(self, valid_df):
        result = ctrl_miss_val_set_cols(valid_df)
        assert result.is_completed()

    def test_missing_values_sabotage(self, error_df):
        result = ctrl_miss_val_set_cols(error_df)
        assert result.is_failed()

    def test_missing_values_collision(self, error_df):
        result = ctrl_miss_val_set_cols(error_df)
        assert result.is_failed()


class TestRenameColumns:
    def test_valid_rename(self):
        df = pd.DataFrame(
            {
                "Date": ["1/1/2023"],
                "Region": ["Moscow"],
                "Location": ["Loc1"],
                "Gps": ["gps1"],
                "Damaged Equipment": ["eq1"],
                "Incident Type": ["type1"],
                "Collision With": ["obj1"],
                "Locomotive Damaged": ["1"],
                "Relay Damaged": ["1"],
                "Partisans Group": ["grp1"],
                "Partisans Arrest": ["arr1"],
                "Partisans Names": ["name1"],
                "Partisans Age": ["25"],
                "Applicable Laws": ["law1"],
                "Source Links": ["link1"],
                "Sabotage Success": ["yes"],
                "Comments": ["comment1"],
                "Exact Date": ["1/1/2023"],
            }
        )
        result = rename_cols(df, CORRESP_SCHEMA_INCIDENT_RAILWAY)
        assert isinstance(result, pd.DataFrame)
