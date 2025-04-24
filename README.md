# arcosparse: A Python library for ARCO sparse datasets subsetting

## Usage

### Main functions

#### `arcosparse.subset_and_return_dataframe`

Subset the data based on the input and return a dataframe.

#### `arcosparse.subset_and_save`

Subset the data based on the input and return data as a partitioned `parquet` file.
It means that the data is saved in one folder and in this folder there are many small `parquet` files. Though, you can open all the data at once.

To open the data into a dataframe, use this snippet:

``` python
import glob

output_path = "some_folder" 

# Get all partitioned Parquet files
parquet_files = glob.glob(f"{output_path}/*.parquet")

# # Read all files into a single dataframe
df = pd.concat(pd.read_parquet(file) for file in parquet_files)
```

## Changelog

### 0.3.5

- Return all the columns even if full of NaNs.

### 0.3.4

- Deleted deprecated `get_platforms_names` function
- Fix an issue when query on the chunk would not be correct if the requested subset is 0.

### 0.3.3

- Add GPLv3 license

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
