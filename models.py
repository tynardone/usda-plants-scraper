import re

from attrs import define, field

_TAG_RE = re.compile(r"<.*?>")


def strip_html(s: str | None) -> str | None:
    return _TAG_RE.sub("", s) if isinstance(s, str) else s


def list_to_str(field: list[str] | None) -> str | None:
    if field is None:
        return None
    return ",".join([str(x) for x in field]) or None


@define(slots=True)
class Plant:
    id: int
    symbol: str
    scientific_name = field(default=None, converter=strip_html)
    common_name: str | None = None
    group: str | None = None
    rank_id: int | None = None
    rank: str | None = None

    has_characteristics: bool
    has_distribution_data: bool
    has_images: bool
    has_related_links: bool
    has_legal_statuses: bool
    has_noxious_statuses: bool

    durations: str | None = field(default=None, converter=list_to_str)
    growth_habits: str | None = field(default=None, converter=list_to_str)
    legal_statuses: str | None = field(default=None, converter=list_to_str)
    noxious_statuses: str | None = field(default=None, converter=list_to_str)


@define(slots=True)
class NativeStatus:
    plant_id: int
    region: str | None = None
    type: str | None = None


@define(slots=True)
class Ancestor:
    id: int
    plant_id: int
    symbol: str
    scientific_name: str | None = field(default=None, converter=strip_html)
    common_name: str | None = None
    rank_id: str | None = None
    rank: str | None = None


@define(slots=True)
class Characteristic:
    plant_id: int
    plant_characteristic_name: str | None = None
    plant_characteristic_value: str | None = None
    plant_characteristic_category: str | None = None
    cultivar_name: str | None = None
    synonym_name: str | None = None
