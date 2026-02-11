from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional, Type, TypeVar, Union, get_args

import pystac

from arcosparse.utils import date_to_timestamp

AssetsNames = Literal["timeChunked", "geoChunked", "platformChunked"]
ASSETS_NAMES = list(get_args(AssetsNames))


class ChunkType(str, Enum):
    ARITHMETIC = "default"
    GEOMETRIC = "symmetricGeometric"


Coordinate_type = TypeVar("Coordinate_type", bound="Coordinate")


@dataclass
class DatasetCoordinate:
    """
    External class to store the metadata of the dataset
    and return it to the user.
    """

    coordinate_id: str
    unit: str
    minimum: Optional[float]
    maximum: Optional[float]
    step: Optional[float]
    values: Optional[list[float]]


@dataclass
class Dataset:
    """
    External class to store the metadata of the dataset
    and return it to the user.
    """

    dataset_id: str
    variables: list[str]
    assets: list[AssetsNames]
    coordinates: list[DatasetCoordinate]


@dataclass
class Coordinate:
    minimum: float
    maximum: float
    step: float
    values: list
    coordinate_id: str
    unit: str
    #: The chunk length can be a single value or a dictionary of possible
    #: values depending on the type of the platform.
    chunk_length: Union[int, dict[str, Optional[int]]]
    chunk_type: ChunkType
    chunk_reference_coordinate: float
    chunk_geometric_factor: float

    @classmethod
    def from_metadata_item(
        cls: Type[Coordinate_type],
        asset: dict,
        coordinate_id: str,
        variable_id: str,
        time_unit: str,
    ) -> Coordinate_type:
        view_dim = asset.get("viewDims", {}).get(coordinate_id, {})
        coordinate_information = view_dim.get("coords", {})
        geometric_factor = view_dim.get("chunkGeometricFactor", 0)
        if isinstance(geometric_factor, dict):
            geometric_factor = geometric_factor.get(variable_id, 0)
        if view_dim.get("chunkLenPerDataType", False):
            chunk_length = view_dim["chunkLen"]
        else:
            chunk_length = view_dim["chunkLen"].get(variable_id, 0)
        # TODO: check validStartDate can be an int? if the time is timestamps
        # TODO: check if we can values or never with insitus?
        return cls(
            minimum=(
                date_to_timestamp(
                    coordinate_information.get("validStartDate")
                    or coordinate_information["min"],
                    time_unit,
                )
                if coordinate_id == "time"
                else coordinate_information["min"]
            ),
            maximum=(
                date_to_timestamp(
                    coordinate_information["max"],
                    time_unit,
                )
                if coordinate_id == "time"
                else coordinate_information["max"]
            ),
            step=coordinate_information["step"],
            values=coordinate_information.get("values"),
            coordinate_id=coordinate_id,
            unit=time_unit if coordinate_id == "time" else view_dim["units"],
            chunk_length=chunk_length,
            chunk_type=ChunkType(view_dim.get("chunkType", "default")),
            chunk_reference_coordinate=view_dim.get("chunkRefCoord"),
            chunk_geometric_factor=geometric_factor,
        )

    def to_dataset_coordinate(self) -> DatasetCoordinate:
        """
        This is useful for returning the metadata to the user.
        """
        return DatasetCoordinate(
            coordinate_id=self.coordinate_id,
            unit=self.unit,
            minimum=self.minimum,
            maximum=self.maximum,
            step=self.step,
            values=self.values,
        )


Variable_type = TypeVar("Variable_type", bound="Variable")


@dataclass
class Variable:
    """
    Variables can have different coordinate chunking in theory.
    At least they don't have always the same coordinates.

    For example, some variable of the dataset might have depth
    others no.
    """

    variable_id: str
    coordinates: list[Coordinate]
    unit: Optional[str]

    @classmethod
    def from_metadata_item(
        cls: Type[Variable_type],
        asset: dict,
        variable_id: str,
        variable_info: dict,
        time_unit: str,
    ) -> Variable_type:
        unit = variable_info.get("unit", None)
        return cls(
            variable_id=variable_id,
            coordinates=[
                Coordinate.from_metadata_item(
                    asset,
                    coordinate_id=coordinate_id,
                    variable_id=variable_id,
                    time_unit=time_unit,
                )
                for coordinate_id in asset.get("viewDims", {}).keys()
            ],
            unit=unit,
        )


Asset_type = TypeVar("Asset_type", bound="Asset")


@dataclass
class Asset:
    """
    The asset from the metadata
    Only loading the variables needed
    """

    asset_id: AssetsNames
    url: str
    variables: list[Variable]

    @classmethod
    def from_metadata_item(
        cls: Type[Asset_type],
        item: pystac.Item,
        variables: list[str],
        asset_name: AssetsNames,
    ) -> Asset_type:
        assets = item.get_assets()
        variable_info = item.properties.get("cube:variables", {})
        time_unit = (
            item.properties.get("cube:dimensions", {})
            .get("time", {})
            .get("cube_units", "seconds since 1970-01-01 00:00:00")
        )

        if not assets or asset_name not in assets:
            raise ValueError(f"Asset {asset_name} not found in the metadata")
        asset = assets[asset_name].to_dict()
        variables_asset = set(variable_info.keys())
        # TODO: add a check that the requested variables exist
        variables_to_parse = variables_asset.intersection(variables)
        if not variables_to_parse:
            raise ValueError(
                f"No variables found in the metadata for {asset_name} asset. "
                f"Requested variables: {variables} "
                f"while available variables: {list(variables_asset)}"
            )
        return cls(
            asset_id=asset_name,
            url=asset["href"],
            variables=[
                Variable.from_metadata_item(
                    asset,
                    variable_id=variable_name,
                    variable_info=variable_info.get(variable_name, {}),
                    time_unit=time_unit,
                )
                for variable_name in variables_to_parse
            ],
        )

    def to_dataset(
        self,
        asset_names: list[AssetsNames],
        dataset_id: str,
    ) -> Dataset:
        """
        This is useful for returning the metadata to the user.
        """
        coordinates_done = set()
        all_coordinates: list[DatasetCoordinate] = []
        for variable in self.variables:
            for coordinate in variable.coordinates:
                if coordinate.coordinate_id not in coordinates_done:
                    coordinates_done.add(coordinate.coordinate_id)
                    all_coordinates.append(coordinate.to_dataset_coordinate())
        return Dataset(
            dataset_id=dataset_id,
            variables=sorted(
                [variable.variable_id for variable in self.variables]
            ),
            assets=asset_names,
            coordinates=all_coordinates,
        )


SQL_COLUMNS = {
    "platformId": 0,
    "platformType": 1,
    "time": 2,
    "longitude": 3,
    "latitude": 4,
    "elevation": 5,
    "pressure": 6,
    "value": 7,
    "valueQc": 8,
}  # kept for information

CHUNK_INDEX_INDICES = {
    "time": 0,
    "elevation": 1,
    "longitude": 2,
    "latitude": 3,
}


@dataclass
class RequestedCoordinate:
    minimum: Optional[float]
    maximum: Optional[float]
    coordinate_id: str


@dataclass
class UserRequest:
    time: RequestedCoordinate
    elevation: RequestedCoordinate
    latitude: RequestedCoordinate
    longitude: RequestedCoordinate
    variables: list[str]
    platform_ids: list[str]


@dataclass
class UserConfiguration:
    disable_ssl: bool = False
    trust_env: bool = True
    ssl_certificate_path: Optional[str] = None
    max_concurrent_requests: int = 10
    https_retries: int = 5
    https_timeout: int = 60
    extra_params: dict[str, str] = field(default_factory=dict)


@dataclass
class OutputCoordinate:
    """
    Class useful to know what data we need
    contrary to the RequestedCoordinate class
    None type is not allowed here.
    """

    minimum: float
    maximum: float
    coordinate_id: str


@dataclass
class ChunksRanges:
    """
    This class is used to store the chunking information
    for the subset we want to download.

    chunk range looks like:
    {
        "time": (0, 5),
        "latitude": (0, 5),
        "longitude": (0, 5),
        "elevation": (0, 5),
    }
    Used as the input of the function to create the names of the chunks
    """

    platform_id: Optional[str]
    variable_id: str
    chunks_ranges: dict[str, tuple[int, int]]
    output_coordinates: list[OutputCoordinate]


@dataclass
class Entity:
    """
    Class used to store the information of the entity
    """

    entity_id: str
    entity_type: str
    institution: Optional[str]
    doi: Optional[str]
