from dateutil.parser._parser import ParserError

from arcosparse.subsetter import DEFAULT_COLUMNS_RENAME, _set_columns_rename
from arcosparse.utils import date_to_timestamp


class TestUtilityFonctions:
    def test_set_columns_rename(self):
        columns_rename_none = None
        columns_rename_empty = {}
        columns_rename_time = {"time": "something"}
        columns_rename_entity_id = {"entity_id": "someotherthing"}
        columns_rename_platform_id = {"platform_id": "donothing"}
        columns_rename_entity_type_and_id = {
            "entity_type": "sometype",
            "entity_id": "someid",
        }

        assert (
            _set_columns_rename(columns_rename_none) == DEFAULT_COLUMNS_RENAME
        )
        assert (
            _set_columns_rename(columns_rename_empty) == DEFAULT_COLUMNS_RENAME
        )
        assert _set_columns_rename(columns_rename_time) == {
            **DEFAULT_COLUMNS_RENAME,
            **columns_rename_time,
        }
        assert _set_columns_rename(columns_rename_entity_id) == {
            "platform_id": "someotherthing",
            "platform_type": "entity_type",
        }
        assert (
            _set_columns_rename(columns_rename_platform_id)
            == DEFAULT_COLUMNS_RENAME
        )
        assert _set_columns_rename(columns_rename_entity_type_and_id) == {
            "platform_id": "someid",
            "platform_type": "sometype",
        }

    def test_date_to_timestamp(self):
        date_str = "2023-10-01T12:00:00Z"
        timestamp_seconds = date_to_timestamp(date_str, "seconds")
        timestamp_milliseconds = date_to_timestamp(date_str, "milliseconds")
        assert timestamp_seconds == 1696161600.0
        assert timestamp_milliseconds == 1696161600000.0

        # Test with a float input
        float_input = 1696166400.0
        assert date_to_timestamp(float_input, "seconds") == float_input
        assert date_to_timestamp(float_input, "milliseconds") == float_input

        # Test other formats
        date_str = "2025-06-25T07:43:54.514180Z"

        timestamp_seconds = date_to_timestamp(date_str, "seconds")
        assert timestamp_seconds == 1750837434.0

        date_str = "2025-06-25"
        timestamp_seconds = date_to_timestamp(date_str, "seconds")
        assert timestamp_seconds == 1750809600

        # Failure cases
        date = "lksdhflk"

        try:
            date_to_timestamp(date, "seconds")
        except ParserError:
            assert True
