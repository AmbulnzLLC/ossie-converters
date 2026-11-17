# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Ossie 0.2 expression forms: a top-level `sql` (OSSIE_SQL_2026), `dialects` entries, or both."""
import xml.etree.ElementTree as ET

import pytest

from ossie.models import OssieDocument
from ossie_tableau import warehouse
from ossie_tableau.tds_writer import SnowflakeConn, write_tds

pytestmark = pytest.mark.unit


def _doc(field_expr: dict, metric_expr: dict) -> OssieDocument:
    return OssieDocument.model_validate({
        "name": "m",
        "datasets": [{"name": "T", "source": "DB.SCH.T", "fields": [
            {"name": "ID", "expression": {"sql": "ID"}, "datatype": "Integer"},
            {"name": "AMT", "expression": field_expr, "datatype": "Decimal"}]}],
        "metrics": [{"name": "total", "expression": metric_expr}],
    })


def _calc(xml: str, caption: str) -> str:
    root = ET.fromstring(xml)
    col = next(c for c in root.iter("column") if c.get("caption") == caption)
    return col.find("calculation").get("formula")


def test_expression_for_reads_sql_and_dialects():
    e = OssieDocument.model_validate({"name": "m", "datasets": [{"name": "T", "source": "S.T", "fields": [
        {"name": "A", "expression": {"sql": "A", "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "B"},
                                                              {"dialect": "TRINO", "expression": "C"}]}}]}]}).datasets[0].fields[0].expression
    assert warehouse.expression_for(e, "OSSIE_SQL_2026") == "A"  # top-level sql is authoritative over an OSSIE_SQL_2026 entry
    assert warehouse.expression_for(e, "TRINO") == "C"
    assert warehouse.expression_for(e, "SNOWFLAKE") is None
    assert warehouse.expression_for(None, "ANSI_SQL") is None


def test_dialect_name_accepts_enum_and_string():
    from ossie.models import OssieDialect
    assert warehouse.dialect_name(OssieDialect.TRINO) == "TRINO"
    assert warehouse.dialect_name("CUSTOM_ENGINE") == "CUSTOM_ENGINE"


def test_sql_only_field_and_metric_convert_without_loss():
    res = write_tds(_doc({"sql": "AMT"}, {"sql": "SUM(T.AMT)"}), SnowflakeConn("acct.example.com"))
    assert [x.kind for x in res.losses] == []
    assert _calc(res.xml, "total") == "SUM([AMT])"


def test_warehouse_dialect_wins_over_portable_sql():
    res = write_tds(_doc({"sql": "AMT"}, {"sql": "COUNT(T.ID)", "dialects": [{"dialect": "SNOWFLAKE", "expression": "SUM(T.AMT)"}]}),
                    SnowflakeConn("acct.example.com"))
    assert _calc(res.xml, "total") == "SUM([AMT])"


def test_ansi_sql_entry_wins_over_portable_sql():
    res = write_tds(_doc({"sql": "AMT"}, {"sql": "COUNT(T.ID)", "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(T.AMT)"}]}),
                    SnowflakeConn("acct.example.com"))
    assert _calc(res.xml, "total") == "SUM([AMT])"
