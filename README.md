# arcosparse: A Python library for ARCO sparse datasets subsetting

## Changelog

### 0.3.2

- Fixes an issue on Windows where deleting a file is not permited if we don't close explicitly the sql connection.

### 0.3.1

- Reindex when concatenate. Fixes issue when indexes wouldn't be unique.
- Fixes an issue on Windows where `datetime.to_timestamp` does not support dates before 1970-1-1 (i.e. negative values for timestamps).
- Fixes an issue on Windows where a temporary sqlite file cannot be opened while it's already open in the process.

### 0.3.0

- Change columns output: from "platform_id" to "entity_id" and from "platform_type" to "entity_type".
- Document the expected column names in the doc of the functions.
- Add `columns_rename` argument to `subset_and_return_dataframe` and `subset_and_save` to be able to choose the names of the columns in the output.
