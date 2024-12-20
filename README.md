# Python sparse subsetter

A subsetter for the MDS sparse data.

## Todos

### Features

- [] Change the input to be the stac assets (or the URL to the stac) and parse it
- [] Choose the best asset for the request
- [] Platform subsetting (use the platform index)
- [] Return the platformIDs available for the dataset to be able to show it to the user and check the platform is correct before subsetting
- [] it would make sense to have default elevation and time windows (otherwise it can be a looooot of chunks)
- [] Estimate the amount of data that will be downloaded (difficult with sparse dataset)
- [] add tests to the repo

### Fixes

-[] Have imports so that pytest and scripts work

## Meeting notes

### 20/12 Guille-Renaud

- Very difficult to evaluate the size of the downloads (cannot be exact)
- Size evaluation: if the number of chunks low enough: do HEAD "content-length" to evaluate the size
- Negative Chunks exist
- Chunks might not exist: 403 if does not exist
- For the platform subset: need to get the platforms index: it has the type of chunking for the platform (get the chunkID and then the chunking)
- chunkLenPerDataType: if true: the chunk length is per data type (each platform id has a type) else per variables as usual
