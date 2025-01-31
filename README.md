# Python sparse subsetter

A subsetter for the MDS sparse data. Based on [tero sparse](https://github.com/lobelia-earth/tero-sparse).

## Todos

### Features

- [x] Change the input to be the stac assets (or the URL to the stac) and parse it
- [x] Choose the best asset for the request
- [] Platform subsetting (use the platform index)
- [] Return the platformIDs available for the dataset to be able to show it to the user and check the platform is correct before subsetting
- [] it would make sense to have default elevation and time windows (otherwise it can be a looooot of chunks)
- [] Estimate the amount of data that will be downloaded (difficult with sparse dataset)
- [] add tests to the repo
- [x] Create proper `requests` sessions to be able use proxies and different configurations
- [] Think of ways to download as much data as possible without out of memory errors (maybe some writing to a local file)
- [] Release and add it to PyPI and conda  (maybe with a on workflow call action to trigger the bump, the push and the commit)
- [x] Add logger

### Fixes

-[x] Have imports so that pytest and scripts work

## Meeting notes

### 20/12 Guille-Renaud

- Very difficult to evaluate the size of the downloads (cannot be exact)
- Size evaluation: if the number of chunks low enough: do HEAD "content-length" to evaluate the size
- Negative Chunks exist
- Chunks might not exist: 403 if does not exist
- For the platform subset: need to get the platforms index: it has the type of chunking for the platform (get the chunkID and then the chunking)
- chunkLenPerDataType: if true: the chunk length is per data type (each platform id has a type) else per variables as usual
