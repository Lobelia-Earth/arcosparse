from arcosparse.subsetter import DEFAULT_COLUMNS_RENAME, _set_columns_rename


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
