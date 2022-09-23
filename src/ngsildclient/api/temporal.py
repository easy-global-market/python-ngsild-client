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
from typing import TYPE_CHECKING, Union, List
from datetime import timedelta
from isodate import duration_isoformat

import logging

if TYPE_CHECKING:
    from .client import Client

from .constants import EntityId, JSONLD_CONTEXT, AggrMethod
from .helper.temporal import TemporalQuery
from ..model.entity import Entity


logger = logging.getLogger(__name__)


def addopt(params: dict, newopt: str):
    if params.get("options", "") == "":
        params["options"] = newopt
    else:
        params["options"] += f",{newopt}"

# TODO : Pagination Support (206 Partial Content + follow Next-Page)

class Temporal:
    def __init__(self, client: Client, url: str):
        self._client = client
        self._session = client.session
        self.url = url

    def get(
        self,
        eid: Union[EntityId, Entity],
        ctx: str = None,
        verbose: bool = False,
        **kwargs,
    ) -> dict:
        eid = eid.id if isinstance(eid, Entity) else eid
        headers = {
            "Accept": "application/ld+json",
            "Content-Type": None,
        }  # overrides session headers
        if ctx is not None:
            headers["Link"] = f'<{ctx}>; rel="{JSONLD_CONTEXT}"; type="application/ld+json"'
        params = {}
        if not verbose:
            addopt(params, "temporalValue")
        r = self._session.get(f"{self.url}/{eid}", headers=headers, params=params, **kwargs)
        self._client.raise_for_status(r)
        return r.json()

    def query(
        self,
        type: str = None,
        attrs: List[str] = None,
        q: str = None,
        gq: str = None,
        ctx: str = None,
        verbose: bool = False,
        tq: TemporalQuery = None,
        lastn: int = 0,
        pagesize: int = 0,  # default broker pageSize
        count: bool = True,
    ) -> List[dict]:
        params = {}
        if type:
            params["type"] = type
        if attrs:
            params["attrs"] = ",".join(attrs)
        if q:
            params["q"] = q
        if gq:
            params["georel"] = gq                 
        if count:
            addopt(params, "count")
        if not verbose:
            addopt(params, "temporalValue")
        if tq is None:
            tq = TemporalQuery().before()
        params |= tq
        if lastn > 0:
            params["lastN"] = lastn
        if pagesize > 0:
            params["pageSize"] = pagesize
        headers = {
            "Accept": "application/ld+json",
            "Content-Type": None,
        }  # overrides session headers
        if ctx is not None:
            headers["Link"] = f'<{ctx}>; rel="{JSONLD_CONTEXT}"; type="application/ld+json"'
        r = self._session.get(
            self.url,
            headers=headers,
            params=params,
        )
        self._client.raise_for_status(r)
        count = int(r.headers["NGSILD-Results-Count"])
        print(f"{count=}")
        return r.json()

    def query_aggr(
        self,
        type: str = None,
        attrs: List[str] = None,
        q: str = None,
        gq: str = None,
        ctx: str = None,
        tq: TemporalQuery = None,
        lastn: int = 0,
        pagesize: int = 0,  # default broker pageSize
        count: bool = False,
        methods: List[AggrMethod] = [AggrMethod.AVERAGE],
        period: timedelta = timedelta(days=1),
    ) -> List[dict]:
        params = {}
        if type:
            params["type"] = type
        if attrs:
            params["attrs"] = ",".join(attrs)            
        if q:
            params["q"] = q
        if gq:
            params["georel"] = gq        
        addopt(params, "aggregatedValues")
        if count:
            addopt(params, "count")
        if tq is None:
            tq = TemporalQuery().before()
        params |= tq
        if lastn > 0:
            params["lastN"] = lastn
        if pagesize > 0:
            params["pageSize"] = pagesize
        params["aggrMethods"] = ",".join([m.value for m in methods])
        params["aggrPeriodDuration"] = duration_isoformat(period)
        headers = {
            "Accept": "application/ld+json",
            "Content-Type": None,
        }  # overrides session headers
        if ctx is not None:
            headers["Link"] = f'<{ctx}>; rel="{JSONLD_CONTEXT}"; type="application/ld+json"'
        r = self._session.get(
            self.url,
            headers=headers,
            params=params,
        )
        self._client.raise_for_status(r)
        count = int(r.headers["NGSILD-Results-Count"])
        print(f"{count=}")
        return r.json()        
