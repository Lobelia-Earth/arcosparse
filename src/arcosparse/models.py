from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional, Type, TypeVar

import pystac

from src.arcosparse.utils import date_to_timestamp


class ChunkType(Enum):
    ARITHMETIC = "default"
    GEOMETRIC = "symmetricGeometric"


Coordinate_type = TypeVar("Coordinate_type", bound="Coordinate")


@dataclass
class Coordinate:
    minimum: float
    maximum: float
    step: float
    values: list
    coordinate_id: str
    unit: str
    chunk_length: int
    chunk_type: ChunkType
    chunk_reference_coordinate: float
    chunk_geometric_factor: float

    @classmethod
    def from_metadata_item(
        cls: Type[Coordinate_type],
        asset: dict,
        coordinate_id: str,
        variable_id: str,
    ) -> Coordinate_type:
        view_dim = asset.get("viewDims", {}).get(coordinate_id, {})
        coordinate_information = view_dim.get("coords", {})
        geometric_factor = view_dim.get("chunkGeometricFactor", 0)
        if isinstance(geometric_factor, dict):
            geometric_factor = geometric_factor.get(variable_id, 0)

        # TODO: check validStartDate can be an int? if the time is timestamps
        # TODO: check if we can values or never with insitus?
        return cls(
            minimum=date_to_timestamp(
                coordinate_information.get("validStartDate")
                or coordinate_information["min"]
            ),
            maximum=date_to_timestamp(coordinate_information["max"]),
            step=coordinate_information["step"],
            values=coordinate_information.get("values"),
            coordinate_id=coordinate_id,
            unit=view_dim["units"],
            chunk_length=view_dim["chunkLen"].get(variable_id, 0),
            chunk_type=ChunkType(view_dim.get("chunkType", "default")),
            chunk_reference_coordinate=view_dim.get("chunkRefCoord"),
            chunk_geometric_factor=geometric_factor,
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
    ) -> Variable_type:
        unit = variable_info.get("unit", None)
        return cls(
            variable_id=variable_id,
            coordinates=[
                Coordinate.from_metadata_item(
                    asset, coordinate_id=coordinate_id, variable_id=variable_id
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

    asset_id: Literal["timeChunked", "geoChunked", "platformChunked"]
    url: str
    variables: list[Variable]

    @classmethod
    def from_metadata_item(
        cls: Type[Asset_type],
        item: pystac.Item,
        variables: list[str],
        asset_name: Literal["timeChunked", "geoChunked", "platformChunked"],
    ) -> Asset_type:
        assets = item.get_assets()
        variable_info = item.properties.get("cube:variables", {})
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
                f"while available variables: {variables_asset}"
            )
        return cls(
            asset_id=asset_name,
            url=asset["href"],
            variables=[
                Variable.from_metadata_item(
                    asset,
                    variable_id=variable_name,
                    variable_info=variable_info.get(variable_name, {}),
                )
                for variable_name in variables_to_parse
            ],
        )


SQL_FIELDS = [
    "platform_id",
    "platform_type",
    "time",
    "longitude",
    "latitude",
    "elevation",
    "pressure",
    "value",
    "value_qc",
]

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
}

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
    coodinate_id: str


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
class ChunksToDownload:
    """
    This class is used to store the chunking information
    for the subset we want to download.
    """

    variable_id: str
    # TODO: subset on platform_id
    # platform_id: str
    chunks_names: set[str]
    output_coordinates: list[OutputCoordinate]
