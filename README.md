# Python sparse subsetter

A subsetter for the MDS sparse data. Based on [tero sparse](https://github.com/lobelia-earth/tero-sparse).

## Todos

### Features

- [x] Change the input to be the stac assets (or the URL to the stac) and parse it
- [x] Choose the best asset for the request
- [x] Platform subsetting (use the platform index)
- [] Return the platformIDs available for the dataset to be able to show it to the user and check the platform is correct before subsetting
- [] it would make sense to have default elevation and time windows (otherwise it can be a looooot of chunks)
- [] Estimate the amount of data that will be downloaded (difficult with sparse dataset)
- [x] Create proper `requests` sessions to be able use proxies and different configurations
- [] Release and add it to PyPI and conda  (maybe with a on workflow call action to trigger the bump, the push and the commit)
- [x] Add logger
- [x] Think of ways to download as much data as possible without out of memory errors (maybe some writing to a local file). We should probably save the result in ORC or parquet (https://medium.com/@aiiaor/which-data-file-format-to-use-csv-json-parquet-avro-orc-e7a9acaaa7df, https://pandas.pydata.org/docs/reference/api/pandas.read_orc.html, https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_orc.html) to be able to write efficiently, then all of this can be saved opened using pandas.
- [] Right now we are doing calls for all the chunks for all the variables, whereas some variable may not exist (especially for the platform id). We might save a lot of computation if we check first that the variable exist (is is possible? since listing might not be authorized)
- [] add tests to the repo
- [] check with Guille if needs to simplify the platform ids (ie from `F-Vartdalsfjorden___MO` to `F-Vartdalsfjorden`)?

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

## Test on the huge downloads

### Test1: full time range

``` python
if __name__ == "__main__":
    request = UserRequest(
        time=RequestedCoordinate(
            minimum=None, maximum=None, coodinate_id="time"
        ),
        latitude=RequestedCoordinate(
            minimum=-63.900001525878906, maximum=90.0, coodinate_id="latitude"
        ),
        longitude=RequestedCoordinate(
            minimum=-146.99937438964844,
            maximum=179.99998474121094,
            coodinate_id="longitude",
        ),
        elevation=RequestedCoordinate(
            maximum=120, minimum=-10, coodinate_id="elevation"
        ),
        variables=["ATMP", "PSAL"],
        platform_ids=[
            "F-Vartdalsfjorden___MO",
            "B-Sulafjorden___MO",
        ],
    )
    url_metadata = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa

    pandas = _subset(
        request,
        user_configuration,
        url_metadata,
        output_directory=Path("todelete"),
        # output_directory=None,
        disable_progress_bar=False,
    )
```

This result in 27MB of parquet file in 2min for 80 concurrent requests (since the requests are small we can push the number of request). Can be ipen and returns a dataframe of 207k lines.

### Test2: full depth and time range

Test using full depth and time range

``` python
if __name__ == "__main__":
    user_configuration = UserConfiguration(
        extra_params={
            "x-cop-client": "copernicus-marine-toolbox",
            "x-cop-client-version": "1.3.4",
            "x-cop-user": "rjester",
        }
    )
    request = UserRequest(
        time=RequestedCoordinate(
            minimum=None, maximum=None, coodinate_id="time"
        ),
        latitude=RequestedCoordinate(
            minimum=-63.900001525878906, maximum=90.0, coodinate_id="latitude"
        ),
        longitude=RequestedCoordinate(
            minimum=-146.99937438964844,
            maximum=179.99998474121094,
            coodinate_id="longitude",
        ),
        elevation=RequestedCoordinate(
            maximum=None, minimum=None, coodinate_id="elevation"
        ),
        variables=["ATMP", "PSAL"],
        platform_ids=[],  
    )
    url_metadata = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa

    pandas = _subset(
        request,
        user_configuration,
        url_metadata,
        output_directory=Path("todelete"),
        # output_directory=None,
        disable_progress_bar=False,
    )

    # for info
    # chunks_ranges = {
    #     "time": (0, 67156),
    #     "longitude": (0, 0),
    #     "latitude": (0, 0),
    #     "elevation": (-9, 9),
    # }  # PSAL

    # # chunks_ranges = {
    # #     "time": (0, 11192),
    # #     "longitude": (0, 0),
    # #     "latitude": (0, 0),
    # #     "elevation": (-2178, 2400),
    # # } # ATMP
```

Problem with variable "ATMP" because we would need to download 51252747 chunks which is huge but also break the python process and it is completely blocked.
Using "PSAL" seems to work with 1275983 of chunks but it would take some 10h to download (in the office, 50 concurrent).
Maybe we should then warn the user that there is a limit in the number of chunks? Or do we need some system to handle this kind of requests?

If we tell the user: "request too big", how can they explore the data and know where the data is?

Some ideas for solution:

- `get_full_chunks_names` should return an iterator
- We can do the computation per batch. (issue: is the progress bar gonna work?)
