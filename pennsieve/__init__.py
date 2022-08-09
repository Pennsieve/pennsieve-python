from warnings import warn

from .client import Pennsieve
from .config import DEFAULTS as DEFAULT_SETTINGS
from .config import Settings
from .models import (
    BaseNode,
    Collection,
    DataPackage,
    Dataset,
    File,
    LinkedModelProperty,
    Model,
    ModelFilter,
    ModelJoin,
    ModelProperty,
    ModelSelect,
    ModelTemplate,
    Organization,
    Property,
    Record,
    RecordSet,
    Relationship,
    RelationshipSet,
    RelationshipType,
    TimeSeries,
    TimeSeriesAnnotation,
    TimeSeriesChannel,
)

warn(
    "Pennsieve is transitioning to the new agent. The majority of the existing classes and functions will be deprecated, API will significantly change; version=7.0.0; date=2022-11-01.",
    DeprecationWarning,
    stacklevel=2,
)

__title__ = "pennsieve"
__version__ = "6.2.0"
