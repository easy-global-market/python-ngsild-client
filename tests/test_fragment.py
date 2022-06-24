#!/usr/bin/env python3

# Software Name: ngsildclient
# SPDX-FileCopyrightText: Copyright (c) 2021 Orange
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Fabien BATTELLO <fabien.battello@orange.com> et al.

from ngsildclient.model.entity import Entity, NESTED
from ngsildclient.model.fragment import Fragment
from ngsildclient.utils.urn import Urn


def test_getitem():
    e = Entity("Vehicle", "A4567").prop("brandName", "Mercedes")
    e.prop("speed", 55, datasetid="Property:speedometerA4567-speed").prop("source", "Speedometer", NESTED)
    assert e._get("speed.value") == 55


def test_append_with_rootattr_1():
    e = Entity("Vehicle", "A4567").prop("brandName", "Mercedes")
    speed1 = (
        Fragment().prop("speed", 55, datasetid="Property:speedometerA4567-speed").prop("source", "Speedometer", NESTED)
    )
    e._append_with_rootattr(speed1)
    assert "speed" in e._payload.keys()
    assert len(e["speed"]) == 1
    assert e["speed"][0] == {
        "type": "Property",
        "value": 55,
        "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
        "source": {"type": "Property", "value": "Speedometer"},
    }


def test_append_with_rootattr_2():
    e = Entity("Vehicle", "A4567").prop("brandName", "Mercedes")
    speed1 = (
        Fragment().prop("speed", 55, datasetid="Property:speedometerA4567-speed").prop("source", "Speedometer", NESTED)
    )
    speed2 = Fragment().prop("speed", 44.5, datasetid="Property:gpsA4567-speed").prop("source", "GPS", NESTED)
    e.append(speed1, speed2)
    assert "speed" in e._payload.keys()
    assert len(e["speed"]) == 2
    assert e["speed"][0] == {
        "type": "Property",
        "value": 55,
        "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
        "source": {"type": "Property", "value": "Speedometer"},
    }
    assert e["speed"][1] == {
        "type": "Property",
        "value": 44.5,
        "datasetId": "urn:ngsi-ld:Property:gpsA4567-speed",
        "source": {"type": "Property", "value": "GPS"},
    }


def test_append_unqualified():
    e = Entity("Vehicle", "A4567").prop("brandName", "Mercedes")
    e.prop("speed", 55, datasetid="Property:speedometerA4567-speed").prop("source", "Speedometer", NESTED)
    speed2 = e["speed"].copy()
    speed2.dotmap.value = 44.5
    speed2.dotmap.source.value = "GPS"
    speed2.dotmap.datasetId = Urn.prefix("Property:gpsA4567-speed")
    e._append_unqualified("speed", speed2)
    assert "speed" in e._payload.keys()
    assert len(e["speed"]) == 2
    assert e["speed"][0] == {
        "type": "Property",
        "value": 55,
        "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
        "source": {"type": "Property", "value": "Speedometer"},
    }
    assert e["speed"][1] == {
        "type": "Property",
        "value": 44.5,
        "datasetId": "urn:ngsi-ld:Property:gpsA4567-speed",
        "source": {"type": "Property", "value": "GPS"},
    }
