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

import json
import requests
import logging

from copy import deepcopy
from functools import partialmethod
from dataclasses import dataclass

from datetime import datetime
from typing import overload, Any, Union, List, Optional, Callable
from rich import print_json

from . import globalsettings
from .exceptions import *
from .constants import *
from .ngsidict import NgsiDict
from .fragment import Fragment
from ngsildclient.utils import iso8601, url, is_interactive
from ngsildclient.utils.urn import Urn

logger = logging.getLogger(__name__)

"""This module contains the definition of the Entity class.
"""


class Entity(Fragment):
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

    A NGSI-LD entity is backed by a NgsiDict object (a custom dictionary that inherits from the native Python dict).
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

    >>> # Add a temporal property named dateObserved
    >>> # We could provide a string if preferred (rather than a datetime)
    >>> e.tprop("dateObserved", datetime(2018, 8, 7, 12))

    >>> # Add a property named NO2 with a pollutant concentration value and a metadata to indicate the unit (mg/m3)
    >>> # The accuracy property is nested
    >>> e.prop("NO2", 22, unitcode="GP", observedat=Auto).prop("accuracy", 0.95, NESTED)

    >>> # Add a relationship towards a POI NGSI-LD Entity
    >>> e.rel("refPointOfInterest", "PointOfInterest:RZ:MainSquare")

    >>> # Pretty-print to standard output
    >>> e.pprint()
    {
        "@context": [
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
            "type": "Relationship",
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

    @overload
    def __init__(self, type: str, id: str, *, ctx: list = [CORE_CONTEXT]):
        """Create a NGSI-LD compliant entity

        One can omit the urn and namespace, "urn:ngsi-ld:" will be added automatically.
        One can omit the type inside the identifier.

        By default, the constructor assumes the identifier naming convention "urn:ngsi-ld:<type>:<remainder>" and automatically
        insert the type into the identifier.
        The default behaviour can be disabled : globalsettings.autoprefix = False.


        Parameters
        ----------
        type : str
            entity type
        id : str
            entity identifier
        ctx : list, optional
            the NGSI-LD context, by default the NGSI-LD Core Context

        Example:
        --------
        >>> from ngsildclient.model.entity import Entity
        >>> e1 = Entity("AirQualityObserved", "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567") # long form
        >>> e2 = Entity("AirQualityObserved", "AirQualityObserved:RZ:Obsv4567") # omit scheme + nss
        >>> e3 = Entity("AirQualityObserved", "RZ:Obsv4567") # omit scheme + nss + type
        >>> print(e1 == e2 == e3)
        True
        >>> e1.pprint()
        {
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ],
            "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
            "type": "AirQualityObserved"
        }
        """
        ...

    @overload
    def __init__(self, id: str, *, ctx: list = [CORE_CONTEXT]):
        """Create a NGSI-LD compliant entity.

        Type is inferred from the fully qualified identifier.
        Works only if your identifiers follow the naming convention "urn:ngsi-ld:<type>:<remainder>"

        Parameters
        ----------
        id : str
            entity identifier (fully qualified urn)
        context : list, optional
            the NGSI-LD context, by default the NGSI-LD Core Context

        Example:
        --------
        >>> from ngsildclient.model.entity import Entity
        >>> e = Entity("urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567")
        >>> e.pprint()
        {
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ],
            "id": "urn:ngsi-ld:AirQualityObserved:RZ:Obsv4567",
            "type": "AirQualityObserved"
        }
        """
        ...

    def __init__(
        self,
        arg1: str = None,
        arg2: str = None,
        *,
        ctx: list = [CORE_CONTEXT],
        payload: dict = None,
        autoprefix: Optional[bool] = None,
    ):
        logger.debug(f"{arg1=} {arg2=}")

        if autoprefix is None:
            autoprefix = globalsettings.autoprefix

        # self._lastprop: NgsiDict = None
        # self._anchored: bool = False

        if payload is not None:  # create Entity from a dictionary
            if not payload.get("id", None):
                raise NgsiMissingIdError()
            if not payload.get("type", None):
                raise NgsiMissingTypeError()
            if not payload.get("@context", None):
                raise NgsiMissingContextError()
            self._payload: NgsiDict = NgsiDict(payload)
            return

        # create a new Entity using its id and type

        if arg2:
            type, id = arg1, arg2
        else:
            type, id = None, arg1

        if type is None:  # try to infer type from the fully qualified identifier
            id = Urn.prefix(id)
            urn = Urn(id)
            if (type := urn.infertype()) is None:
                raise NgsiMissingTypeError(f"{urn.fqn=}")
        else:  # type is not None
            autoprefix &= not Urn.is_prefixed(id)
            if autoprefix:
                bareid = Urn.unprefix(id)
                prefix = f"{type}:"
                if not bareid.startswith(prefix):
                    id = prefix + bareid
            id = Urn.prefix(id)  # set the prefix "urn:ngsi-ld:" if not already done
            urn = Urn(id)

        super().__init__(NgsiDict({"@context": ctx, "id": urn.fqn, "type": type}))

    @property
    def id(self):
        return self._payload["id"]

    @id.setter
    def id(self, eid: str):
        self._payload["id"] = eid

    @property
    def type(self):
        return self._payload["type"]

    @type.setter
    def type(self, etype: str):
        self._payload["type"] = etype

    @property
    def context(self):
        return self._payload["@context"]

    @context.setter
    def context(self, ctx: list):
        self._payload["@context"] = ctx

    def is_root_fragment(self) -> bool:
        return True

    @classmethod
    def load(cls, filename: str):
        """Load an Entity from a JSON file, locally from the filesystem or remotely through HTTP.

        For convenience some `SmartDataModels <https://smartdatamodels.org/>`_ examples are made available thanks to the Smart Data Models initiative.
        You can benefit from autocompletion to navigate inside the available datamodels.

        Parameters
        ----------
        filename : str
            If filename corresponds to an URL, the JSON file is downloaded from HTTP.
            Else it is retrieved locally from the filesystem.

        Returns
        -------
        Entity
            The Entity instance

        See Also
        --------
        model.constants.SmartDataModels

        Example:
        --------
        >>> from ngsildclient import *
        >>> e = Entity.load(SmartDatamodels.SmartCities.Weather.WeatherObserved)
        """
        if url.isurl(filename):
            resp = requests.get(filename)
            payload = resp.json()
        else:
            with open(filename, "r") as fp:
                payload = json.load(fp)
        if isinstance(payload, List):
            return [cls.from_dict(x) for x in payload]
        return cls.from_dict(payload)

    @classmethod
    def load_batch(cls, filename: str):
        """Load a batch of entities from a JSON file.

        Parameters
        ----------
        filename : str
            The input file must contain a JSON array

        Returns
        -------
        List[Entity]
            A list of entities

        Example:
        --------
        >>> from ngsildclient import *
        >>> rooms = Entity.load_batch("/tmp/rooms_all.jsonld")
        """
        with open(filename, "r") as fp:
            payload = json.load(fp)
        if not isinstance(payload, List):
            raise ValueError("The JSON payload MUST be an array")
        return [cls.from_dict(x) for x in payload]

    def save(self, filename: str, *, indent: int = 2):
        """Save the entity to a file.

        Parameters
        ----------
        filename : str
            Name of the output file
        indent : int, optional
            identation size (number of spaces), by default 2
        """
        with open(filename, "w") as fp:
            json.dump(self._payload, fp, default=str, ensure_ascii=False, indent=indent)

    @classmethod
    def save_batch(cls, entities: List[Entity], filename: str, *, indent: int = 2):
        """Save a batch of entities to a JSON file.

        Parameters
        ----------
        entities: List[Entity]
            Batch of entities to be saved

        filename : str
            If filename corresponds to an URL, the JSON file is downloaded from HTTP.
            Else it is retrieved locally from the filesystem.

        Example:
        --------
        >>> from ngsildclient import *
        >>> rooms = [Entity("Room", "Room1"), Entity("Room", "Room2")]
        >>> Entity.save_batch(rooms, "/tmp/rooms_all.jsonld")
        """
        payload = [x._payload for x in entities]
        with open(filename, "w") as fp:
            json.dump(payload, fp, default=str, ensure_ascii=False, indent=indent)
