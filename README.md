# arcosparse: A Python library for ARCO sparse datasets subsetting

## Changelog

### 0.3.0

- Change columns output: from "platform_id" to "entity_id" and from "platform_type" to "entity_type".
- Document the expected column names in the doc of the functions.
- Add `columns_rename` argument to `subset_and_return_dataframe` and `subset_and_save` to be able to choose the names of the columns in the output.
