# arcosparse: A Python library for ARCO sparse datasets subsetting

> [!WARNING]
> This library is still in development. Breaking changes might be introduced from version `0.y.z` to `0.y+1.z`.

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

#### `arcosparse.get_entities`

A function to get the metadata about the entities that are available in the dataset. Since all the information is retrieved from the metadata, the argument is the `url_metadata`, the same used for the subset.
Returns a list of `Entity`: class that can be easily imported from the arcosparse module `from arcosparse import Entity`. It contains information about the entities available in the dataset:

- `entity_id`: same as the `entity_id` column in the result of a subset.
- `entity_type`: same as the `entity_type` column in the result of a subset.
- `doi`: the DOI of the entity.
- `institution`: the institution associated with the entity.

## Changelog

### 0.4.0

#### Breaking Changes

- Deleted function `get_entities_ids`. Use `get_entities` as a replacement. Example:

``` python
# old code
my_entities = get_entities_ids(url_metadata)

# new code
my_entities = [entity.entity_id for entity in get_entities(url_metadata)]
```

#### New features

- Added function `get_entities`. It returns a list of `Entity` objects.

#### Bug fixes

- Fix a bug where arcosparse would modify the dict that users input in the `columns_rename` argument. Now, it deepcopy it to modify it after that.

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
