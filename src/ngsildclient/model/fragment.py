#!/usr/bin/env python3

# Software Name: ngsildclient
# SPDX-FileCopyrightText: Copyright (c) 2021 Orange
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Fabien BATTELLO <fabien.battello@orange.com> et al.

from __future__ import annotations

import logging
import regex
from copy import deepcopy
from functools import partialmethod

from datetime import datetime
from typing import Any, Union, List
from dotmap import DotMap

from .exceptions import *
from .constants import *
from .ngsidict import NgsiDict

logger = logging.getLogger(__name__)

PATTERN = regex.compile(r"(?P<key>\w+){1}(?P<index>\[\d+\])*")

"""This module contains the definition of the Entity class.
"""


class Fragment:
    """The main goal of this class is to build, manipulate and represent a NGSI-LD compliant entity.

    The preferred constructor allows to create an entity from its NGSI-LD type and identifier (and optionally context),
    If no context is provided, it defaults to the NGSI-LD Core Context.
    The identifier can be written using the "long form" : the fully qualified urn string.
    It can also be shorten :
    - omit the urn prefix (scheme+nss)
    - omit the type inside the identifier, assuming the naming convention "urn:ngsi-ld:<type>:<remainder>"

    An alternate constructor allows to create an entity by just providing its id (and optionally context).
    In this case the type is inferred from the fully qualified urn string,
    assuming the naming convention "urn:ngsi-ld:<type>:<remainder>".

    The load() classmethod allows to load a NGSI-LD entity from a file or remotely through HTTP.
    For convenience some `SmartDataModels <https://smartdatamodels.org/>`_ examples are made available.

    Once initiated, we can build a complete NGSI-LD entity by adding attributes to the NGSI-LD entity.
    An attribute can be a Property, TemporalProperty, GeoProperty or RelationShip.
    Methods prop(), tprop(), gprop() and rel() are used to respectively build a Property, TemporalProperty,
    GeoProperty, and Relationship.
    Attributes can carry metadatas, such as "observedAt".
    Attributes can carry user data.

    Nested attributes are supported.
    Methods prop(), tprop(), gprop() are chainable, allowing to build nested properties.

    Dates and Datetimes are ISO8601.
    Helper functions are provided in the module utils.iso8601.
    Often a same date (the one when the measure/event happened) is used many times in the entity.
    When building an entity, the first time a datetime is used it is cached, then can be reused using "Auto".

    Given a NGSI-LD entity, many actions are possible :
    - access/add/remove/update attributes
    - access/update/remove values
    - print the content in the normalized or simplified (aka KeyValues) flavor
    - save the entity to a file
    - send it to the NGSI-LD Context Broker for creation/update (use the ngsildclient.api package)

    A NGSI-LD entity is backed by a NgsgiDict object (a custom dictionary that inherits from the native Python dict).
    So if for any reasons you're stuck with the library and cannot achieve to build a NGSI-LD entity
    that fully matches your target datamodel, it's always possible to manipulate directly the underlying dictionary.

    Raises
    ------
    NgsiMissingIdError
        The identifier is missing
    NgsiMissingTypeError
        The type is missing
    NgsiMissingContextError
        The context is missing

    See Also
    --------
    model.NgsiDict : a custom dictionary that inherits from the native dict and provides primitives to build attributes
    api.client.Client : the NGSI-LD Context Broker client to interact with a Context Broker

    Example:
    --------
    >>> from datetime import datetime
    >>> from ngsildclient import *

    >>> # Create the entity
    >>> e = Entity("AirQualityObserved", "RZ:Obsv4567")


    >>> # Add a property named NO2 with a pollutant concentration value and a metadata to indicate the unit (mg/m3)
    >>> # The accuracy property is nested
    >>> e.prop("NO2", 22, unitcode="GP", observedat=Auto).prop("accuracy", 0.95, NESTED)

    >>> # Add a relationship towards a POI NGSI-LD Entity
    >>> e.rel("refPointOfInterest", "Pogld"
        ],
        "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
        "type": "AirQualityObserved",
        "dateObserved": {
            "type": "Property",
            "value": {
                "@type": "DateTime",
                "@value": "2018-08-07T12:00:00Z"
            }
        },
        "NO2": {
            "type": "Property",
            "value": 22,
            "unitCode": "GP",
            "observedAt": "2018-08-07T12:00:00Z",
            "accuracy": {
                "type": "Property",
                "value": 0.95
            }
        },
        "refPointOfInterest": {
            "type": "Relationship",False
            "object": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare"
        }
    }

    >>> # Update a property by overriding it
    >>> e.prop("dateObserved", iso8601.utcnow())

    >>> # Update a value using the dot notation
    >>> e["NO2.accuracy.value"] = 0.96

    >>> # Remove a property
    >>> e.rm("NO2.accuracy")
    """

    def __init__(self, payload: dict = {}, shallow=True):
        """Create a NGSI-LD entity from a dictionary.

        The input dictionary must at least contain the 'id', 'type' and '@context'.
        This method assumes that the input dictionary matches a valid NGSI-LD structure.

        Parameters
        ----------
        payload : dict
            The given dictionary.

        Returns
        -------
        Entity
            The result Entity instance
        """
        self._lastprop: NgsiDict = None
        self._anchored: bool = False

        if isinstance(payload, NgsiDict):
            self._payload = payload if shallow else deepcopy(payload)
        else:
            self._payload = NgsiDict(payload)

    def is_root_fragment(self) -> bool:
        return False

    @property
    def dotmap(self) -> DotMap:
        return self._payload

    def hasroot(self) -> bool:
        if self.is_root_fragment():
            return False
        return "type" not in self._payload.keys()

    @property
    def rootattr(self) -> str:
        if self.hasroot():
            try:
                return [*self._payload.keys()][0]
            except (KeyError, IndexError):
                return None
        return None

    @classmethod
    def from_dict(cls, payload: dict):
        """Create a NGSI-LD entity from a dictionary.

        The input dictionary must at least contain the 'id', 'type' and '@context'.
        This method assumes that the input dictionary matches a valid NGSI-LD structure.

        Parameters
        ----------
        payload : dict
            The given dictionary.

        Returns
        -------
        Entity
            The result Entity instance
        """
        return cls(payload=payload)

    @classmethod
    def duplicate(cls, fragment: Fragment) -> Fragment:
        """Duplicate a given entity.

        Parameters
        ----------
        entity : Entity
            The input Entity

        Returns
        -------
        Entity
            The output entity
        """
        new = deepcopy(fragment)
        return new

    def copy(self) -> Fragment:
        """Duplicates the entity

        Returns
        -------
        Entity
            The new entity
        """
        return Fragment.duplicate(self)

    def _get(self, key: str) -> Any:
        try:
            current = self._payload
            parts = key.split(".")
            for p in parts:
                m = PATTERN.match(p)
                k = m["key"]
                current = current[k]
                for ix in m.captures("index"):
                    i = int(ix[1:-1])
                    current = current[i]
            return current
        except Exception:
            raise KeyError(key)

    def _set(self, key: str, value: Any):
        try:
            old = None
            current = self._payload
            parts = key.split(".")
            for p in parts:
                m = PATTERN.match(p)
                k = m["key"]
                old = current
                current = current[k]
                for ix in m.captures("index"):
                    i = int(ix[1:-1])
                    old = current
                    current = current[i]
            print(f"{current=}, {old=}")
            lastkey = key.rsplit(".")[-1]
            try:
                index = lastkey.index("[")
                lastkey = lastkey[:index]
            except ValueError:
                pass
            old[lastkey] = value
        except Exception:
            raise KeyError(key)

    def __getitem__(self, key: str) -> Fragment:
        item = self._get(key)
        return Fragment(item) if isinstance(item, dict) else item

    def __setitem__(self, key: str, value: Any):
        self._payload[key] = value

    def __delitem__(self, key: str):
        del self._payload[key]

    def __or__(self, other):
        if isinstance(other, Fragment) and other.hasroot():
            return self._payload | other._payload
        return self

    def __ior__(self, other):
        if isinstance(other, Fragment) and other.hasroot():
            self._payload |= other._payload
        elif isinstance(other, dict):
            self._payload |= other
        return self

    def _append_unqualified(self, rootattr: str, value: Fragment):
        if value.hasroot():
            raise ValueError("Value already has a root attribute")
        try:
            item = self[rootattr]
        except KeyError:
            self[rootattr] = [value.to_dict()]
            return
        if not isinstance(item, List):
            self[rootattr] = [item.to_dict()]
        if isinstance(value, Fragment):
            value = value.to_dict()
        self[rootattr].append(value)

    def _append_with_rootattr(self, value: Fragment):
        if not value.hasroot():
            raise ValueError("Value must have a root attribute")
        print(f"{value.rootattr=}")
        self._append_unqualified(value.rootattr, value[value.rootattr])

    def append(self, *values, attr: str = None):
        for v in values:
            if v.hasroot():
                self._append_with_rootattr(v)
            else:
                self._append_unqualified(attr, v)

    def jsonpath(self, path: str) -> Fragment:
        return self._payload._jsonpath(path)

    def anchor(self):
        """Set an anchor.

        Allow to specify that the last property is used as an anchor.
        Once the anchor property is specified, new properties are attached to the anchor property.

        Parameters
        ----------
        entity : Entity
            The input Entity

        Returns
        -------
        Entity
            The output entity

        Example:
        --------
        Here an anchor is set at the "availableSpotNumber" property.
        Hence the "reliability" and "providedBy" properties are attached to (nested in) the "availableSpotNumber" property.
        Without anchoring, the "reliability" and "providedBy" properties would apply to the entity's root.

        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("OffStreetParking", "Downtown1")
        >>> e.prop("availableSpotNumber", 121, observedat=datetime(2017, 7, 29, 12, 5, 2)).anchor()
        >>> e.prop("reliability", 0.7).rel("providedBy", "Camera:C1").unanchor()
        >>> e.pprint()
        {
            "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "http://example.org/ngsi-ld/parking.jsonld"
            ],
            "id": "urn:ngsi-ld:OffStreetParking:Downtown1",
            "type": "OffStreetParking",
            "availableSpotNumber": {
                "type": "Property",
                "value": 121,
                "observedAt": "2017-07-29T12:05:02Z",
                "reliability": {
                    "type": "Property",
                    "value": 0.7
                },
                "providedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Camera:C1"
                }
            }
        }
        """
        self._anchored = True
        return self

    def unanchor(self):
        """Remove the anchor.

        See Also
        --------
        Entity.anchor()
        """

        self._anchored = False
        return self

    def _update(self, attrname: str, property: NgsiDict, nested: bool = False):
        nested |= self._anchored
        if nested and self._lastprop is not None:
            # update _lastprop only if not anchored
            self._lastprop[attrname] = property
            if not self._anchored:
                self._lastprop = property
        else:
            self._lastprop = self._payload[attrname] = property

    def prop(
        self,
        name: str,
        value: Any,
        nested: bool = False,
        *,  # keyword-only arguments after this
        unitcode: str = None,
        observedat: Union[str, datetime] = None,
        datasetid: str = None,
        userdata: NgsiDict = NgsiDict(),
        escape: bool = False,
    ) -> Fragment:
        """Build a Property.

        Build a property and attach it to the current entity.
        One can chain prop(),tprop(), gprop(), rel() methods to build nested properties.

        Parameters
        ----------
        name : str
            the property name
        value : Any
            the property value
        observedat : Union[str, datetime], optional
            observetAt metadata, timestamp, ISO8601, UTC, by default Noneself._update_entity(name, property, nested)
        userdata : NgsiDict, optional
            a dict or NgsiDict containing user data, i.e. userdata={"reliability": 0.95}, by default NgsiDict()
        escape : bool, optional
            if set escape the string value (useful if contains forbidden characters), by default False

        Returns
        -------
        Fragment
            The updated fragment

        Example:
        --------
        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("AirQualityObserved", "RZ:Obsv4567")
        >>> e.prop("NO2", 22, unitcode="GP") # basic property
        {'type': 'Property', 'value': 22, 'unitCode': 'GP'}
        >>> e.prop("PM10", 18, unitcode="GP").prop("reliability", 0.95) # chain methods to obtain a nested property
        {'type': 'Property', 'value': 0.95}
        >>> e.pprint()
        {
        "@context": [
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        ],
        "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
        "type": "AirQualityObserved",
        "NO2": {
            "type": "Property",
            "value": 22,
            "unitCode": "GP"
        },
        "PM10": {
            "type": "Property",
            "value": 18,
            "unitCode": "GP",
            "reliability": {
                "type": "Property",
                "value": 0.95
            }
        }
        """
        property = self._payload._build_property(value, unitcode, observedat, datasetid, userdata, escape)
        self._update(name, property, nested)
        return self

    def addr(self, value: str):
        return self.prop("address", value)

    def gprop(
        self,
        name: str,
        value: NgsiGeometry,
        nested: bool = False,
        observedat: Union[str, datetime] = None,
        datasetid: str = None,
    ) -> Fragment:
        """Build a GeoProperty.

        Build a GeoProperty and attach it to the current entity.
        One can chain prop(),tprop(), gprop(), rel() methods to build nested properties.

        Parameters
        ----------
        name : str
            the property name
        value : NgsiGeometry
            the property value

        Returns
        -------
        Fragment
            The updated fragment

        Example:
        --------
        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("PointOfInterest", "RZ:MainSquare")
        >>> e.prop("description", "Beach of RZ")
        >>> e.gprop("location", (44, -8))
        >>> e.pprint()
        {
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ],
            "id": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare",
            "type": "PointOfInterest",
            "description": {
                "type": "Property",
                "value": "Beach of RZ"
            },
            "location": {datasetid
                "type": "GeoProperty",
                "value": {
                "type": "Point",
                "coordinates": [
                    -8,datasetid
            }
        }
        """
        property = self._payload._build_geoproperty(value, observedat, datasetid)
        self._update(name, property, nested)
        return self

    loc = partialmethod(gprop, "location")
    """ A helper method to set the frequently used "location" geoproperty.

    entity.loc((44, -8)) is a shorcut for entity.gprop("location", (44, -8))
    """

    def tprop(self, name: str, value: NgsiDate = None, nested: bool = False) -> Fragment:
        """Build a TemporalProperty.

        Build a TemporalProperty and attach it to the current entity.
        One can chain prop(),tprop(), gprop(), rel() methoddatasetids to build nested properties.

        Parameters
        ----------
        name : str
            the property name
        value : NgsiDate
            the property value, utcnow() if None

        Returns
        -------
        Entity
            The updated entity

        Example:
        --------
        >>> from datetime import datetime
        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("AirQualityObserved", "RZ:Obsv4567")
        >>> e.tprop("dateObserved", datetime(2018, 8, 7, 12))
        >>> e.pprint()
        {
            "@context": [datasetid
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ],
            "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
            "type": "AirQualityObserved",
            "dateObserved": {
                "type": "Property",
                "value": {
                    "@type": "DateTime",
                    "@value": "2018-08-07T12:00:00Z"
                }
            }
        }
        """
        property = self._payload._build_temporal_property(value)
        self._update(name, property, nested)
        return self

    obs = partialmethod(tprop, "dateObserved")
    """ A helper method to set the frequently used "dateObserved" property.

    entity.obs("2022-01-12T12:54:38Z") is a shorcut for entity.tprop("dateObserved", "2022-01-12T12:54:38Z")
    """

    def rel(
        self,
        name: Union[Rel, str],
        value: Union[str, List[str], Fragment, List[Fragment]],
        nested: bool = False,
        *,
        observedat: Union[str, datetime] = None,
        datasetid: str = None,
        userdata: NgsiDict = NgsiDict(),
    ) -> Fragment:
        """Build a Relationship Property.

        Build a Relationship Property and attach it to the current entity.
        One can chain prop(),tprop(), gprop(), rel() methods to build nested properties.

        Parameters
        ----------
        name : str
            the property name
        value : str
            the property value

        Returns
        -------
        Entity
            The updated entity

        Example:
        --------
        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("AirQualityObserved", "RZ:Obsv4567")
        >>> e.rel("refPointOfInterest", "PointOfInterest:RZ:MainSquare")
        >>> e.pprint()
        {
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ],
            "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
            "type": "AirQualityObserved",
            "refPointOfInterest": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare"
            }
        }
        """
        if isinstance(name, Rel):
            name = name.value

        if isinstance(value, List):
            property = self._payload._m_build_relationship(value, observedat, datasetid, userdata)
        else:
            property = self._payload._build_relationship(value, observedat, datasetid, userdata)

        self._update(name, property, nested)
        return self

    def __eq__(self, other: Fragment):
        if other.__class__ is self.__class__:
            return self._payload == other._payload
        elif isinstance(other, (NgsiDict, dict)):
            return self._payload == other
        else:
            return NotImplemented

    def __repr__(self):
        return self._payload.__repr__()

    def to_dict(self) -> NgsiDict:
        """Returns the entity as a dictionary.

        The returned type is NgsiDict, fully compatible with a native dict.

        Parameters
        ----------
        kv : bool, optional
            KeyValues format (aka simplified representation), by default False

        Returns
        -------
        NgsiDict
            The underlying native Python dictionary
        """
        return self._payload.toDict()

    def to_json(self, withroot=False, *args, **kwargs) -> str:
        """Returns the entity as JSON.

        Parameters
        ----------
        kv : bool, optional
            KeyValues format (aka simplified representation), by default False

        Returns
        -------
        str
            The JSON content
        """
        return self._payload.to_json(*args, **kwargs)

    def pprint(self, *args, **kwargs):
        """Pretty-print the entity to the standard ouput.

        Parameters
        ----------
        kv : bool, optional
            KeyValues format (aka simplified representation), by default False
        """
        return self._payload.pprint(*args, **kwargs)
