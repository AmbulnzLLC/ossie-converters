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

import dataclasses
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from ossie.models import OssieDocument
from ossie_tableau import warehouse
from ossie_tableau.cli import main
from ossie_tableau.tds_writer import AthenaConn, BigQueryConn, SnowflakeConn, write_tds

pytestmark = pytest.mark.unit


def _bigquery_fixture() -> Path:
    return Path(__file__).parent / "fixtures" / "synthetic_bigquery" / "synthetic_bigquery.tds"

DOC = OssieDocument.model_validate({
    "name": "m",
    "datasets": [{"name": "T", "source": "DB.SCH.T", "fields": [
        {"name": "ID", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "ID"}]}, "datatype": "Integer"}]}],
})


def test_registry_and_lookup():
    assert warehouse.get("snowflake") is warehouse.SNOWFLAKE is warehouse.DEFAULT
    assert warehouse.by_tableau_class("snowflake") is warehouse.SNOWFLAKE
    assert warehouse.by_tableau_class("postgres") is None
    assert warehouse.SNOWFLAKE.dialect_preference == ("SNOWFLAKE", "ANSI_SQL", "OSSIE_SQL_2026")


def test_unknown_warehouse_is_a_clear_error():
    with pytest.raises(ValueError, match="unknown warehouse 'oracle'.*snowflake"):
        warehouse.get("oracle")


def test_snowflake_conn_factory_builds_a_connection():
    c = SnowflakeConn("a.snowflakecomputing.com", "WH", "DB", "SCH", "R", "U")
    assert c == warehouse.Connection(warehouse.SNOWFLAKE, "a.snowflakecomputing.com", "DB", "SCH", "R", "U", {"warehouse": "WH"})


def test_snowflake_connection_block():
    xml = write_tds(DOC, SnowflakeConn("a.snowflakecomputing.com", "WH", "DB", "SCH", "R", "U")).xml
    conn = ET.fromstring(xml).find(".//named-connection/connection")
    assert dict(conn.attrib) == {
        "authentication": "Username Password", "class": "snowflake", "dbname": "DB", "odbc-connect-string-extras": "",
        "one-time-sql": "", "schema": "SCH", "server": "a.snowflakecomputing.com", "service": "R", "username": "U", "warehouse": "WH"}
    assert conn.find("connection-customization") is None
    assert ET.fromstring(xml).find(".//named-connection").get("name").startswith("snowflake.")


def test_descriptor_drives_the_connection_class():
    pg = dataclasses.replace(warehouse.SNOWFLAKE, name="pg", tableau_class="postgres", sqlglot_dialect="postgres",
                             ossie_dialect="ANSI_SQL", extra_attributes=())
    xml = write_tds(DOC, warehouse.Connection(pg, "h", "DB", "SCH", "R", "U")).xml
    node = ET.fromstring(xml).find(".//named-connection")
    assert node.get("name").startswith("postgres.")
    assert node.find("connection").get("class") == "postgres"
    assert "warehouse" not in node.find("connection").attrib


def test_cli_rejects_unknown_warehouse_type(tmp_path, capsys):
    with pytest.raises(SystemExit):
        main(["to-tds", "x.yaml", "-o", str(tmp_path), "--warehouse-type", "oracle"])
    assert "invalid choice" in capsys.readouterr().err


def test_bigquery_registry_and_lookup():
    assert warehouse.get("bigquery") is warehouse.BIGQUERY
    assert warehouse.by_tableau_class("bigquery") is warehouse.BIGQUERY
    assert warehouse.BIGQUERY.dialect_preference == ("BIGQUERY", "ANSI_SQL", "OSSIE_SQL_2026")


def test_bigquery_connection_block():
    xml = write_tds(DOC, BigQueryConn("my-proj", "ds", "bill-proj", "u@example.com")).xml
    node = ET.fromstring(xml).find(".//named-connection")
    assert node.get("name").startswith("bigquery.")
    attrs = dict(node.find("connection").attrib)
    assert attrs["class"] == "bigquery"
    assert attrs["CATALOG"] == "my-proj"
    assert attrs["EXECCATALOG"] == "bill-proj"
    assert attrs["project"] == "my-proj"
    assert attrs["schema"] == "ds"
    assert attrs["connection-dialect"] == "google-sql"
    for omitted in ("dbname", "warehouse", "service", "one-time-sql"):
        assert omitted not in attrs


def test_bigquery_billing_project_defaults_to_data_project():
    c = BigQueryConn("p", "d")
    assert c.extra["EXECCATALOG"] == "p"


def test_bigquery_fixture_translates_to_bigquery_sql():
    from ossie_tableau.convert import convert_file

    (r,) = convert_file(_bigquery_fixture())
    ds = r.document.datasets[0]
    assert ds.source == "synth-project.synth_dataset.ORDERS"
    exprs = {f.name: {d.dialect: d.expression for d in f.expression.dialects} for f in ds.fields}
    assert exprs["Order_Month"]["BIGQUERY"] == "DATETIME_TRUNC(CAST((ORDER_DATE) AS DATETIME), MONTH)"
    assert exprs["Has_Note"]["BIGQUERY"] == "(INSTR((NOTE), 'rush') > 0)"
    assert "SNOWFLAKE" not in {d for e in exprs.values() for d in e}
    assert {x.kind for x in r.losses} >= {"source_qualified", "expression_approximated"}
    assert [m.name for m in r.document.metrics] == ["Total_Amount"]


def test_bigquery_ansi_quoted_identifiers_are_not_string_literals(tmp_path):
    from ossie_tableau.convert import convert_file

    src = _bigquery_fixture().read_text().replace("ORDER_DATE", "Ship Date")
    p = tmp_path / "spaces.tds"
    p.write_text(src)
    (r,) = convert_file(p, warehouse=warehouse.BIGQUERY)
    month = next(f for f in r.document.datasets[0].fields if f.name == "Order_Month")
    bq = next(d.expression for d in month.expression.dialects if d.dialect == "BIGQUERY")
    assert "'Ship Date'" not in bq
    assert "Ship Date" in bq


def test_bigquery_round_trip_keeps_every_expression(tmp_path):
    from ossie_tableau.convert import convert_file

    (a,) = convert_file(_bigquery_fixture())
    out = tmp_path / "rt.tds"
    out.write_text(write_tds(a.document, BigQueryConn("synth-project", "synth_dataset")).xml)
    (b,) = convert_file(out)

    def exprs(r):
        return {f.name: sorted((d.dialect, d.expression) for d in f.expression.dialects)
                for f in r.document.datasets[0].fields} | {
            m.name: sorted((d.dialect, d.expression) for d in m.expression.dialects) for m in r.document.metrics}

    assert exprs(a) == exprs(b)
    assert b.document.datasets[0].source == a.document.datasets[0].source


def test_bigquery_categorical_group_escapes_quotes_for_bigquery():
    from ossie_tableau.convert import convert_datasource
    from tests.test_units import BASE, ds

    xml = ("<column name='[G]' caption='Grp' datatype='string' role='dimension'>"
           "<calculation class='categorical-bin' column='[name]' new-bin='true'>"
           "<bin value='&quot;A&quot;'><value>&quot;It&apos;s&quot;</value></bin></calculation></column>")
    r = convert_datasource(ds(BASE, xml), warehouse=warehouse.BIGQUERY)
    by = {d.dialect: d.expression for d in next(f for f in r.document.datasets[0].fields if f.name == "Grp").expression.dialects}
    assert "It''s" in by["ANSI_SQL"]
    assert "It\\'s" in by["BIGQUERY"] and "''s" not in by["BIGQUERY"]


def test_bigquery_date_of_us_string_literal_is_rewritten_to_iso():
    from ossie_tableau.calc_parser import to_sql

    assert to_sql('DATE("1/2/2015")', wh=warehouse.BIGQUERY) == "CAST('2015-01-02' AS DATE)"
    assert to_sql('DATE("2015-01-02")', wh=warehouse.BIGQUERY) == "CAST('2015-01-02' AS DATE)"
    assert to_sql("#1/2/2015#", wh=warehouse.BIGQUERY) == "CAST('2015-01-02' AS DATE)"


def test_reserved_word_columns_are_quoted():
    from ossie_tableau.calc_parser import to_sql

    assert to_sql("[Order] + 1", wh=warehouse.BIGQUERY) == '"Order" + 1' or to_sql("[Order] + 1", wh=warehouse.BIGQUERY) == "`Order` + 1"
    assert warehouse.needs_quote("Group") and warehouse.needs_quote("end") and not warehouse.needs_quote("Region")


def test_bigquery_mod_of_floor_is_cast_to_int64():
    from ossie_tableau.calc_parser import to_sql

    out = to_sql("FLOOR([a] / 60) % 60", wh=warehouse.BIGQUERY)
    assert "CAST(" in out and "INT64" in out and "MOD(" in out
    assert "INT64" not in to_sql("[a] % 7", wh=warehouse.BIGQUERY)
    assert "INT64" not in to_sql("FLOOR([a] / 60) % 60", wh=warehouse.SNOWFLAKE)


def test_bigquery_numeric_function_on_text_operand_uses_safe_cast():
    from ossie_tableau.calc_parser import to_sql, translate

    out = to_sql("INT(LEFT([s], 2))", wh=warehouse.BIGQUERY)
    assert "SAFE_CAST(" in out and "FLOAT64" in out
    assert any("SAFE_CAST" in n for n in translate("INT(LEFT([s], 2))", wh=warehouse.BIGQUERY).notes)
    assert "SAFE_CAST" not in to_sql("INT([n])", wh=warehouse.BIGQUERY)
    assert "TRY_CAST" not in to_sql("INT(LEFT([s], 2))", wh=warehouse.SNOWFLAKE)


def test_bigquery_split_negative_token_counts_from_the_end():
    from ossie_tableau.calc_parser import to_sql

    assert "ARRAY_REVERSE(SPLIT(s, '$'))[SAFE_OFFSET(0)]" in to_sql("SPLIT([s], '$', -1)", wh=warehouse.BIGQUERY)
    assert "SAFE_OFFSET(1)" in to_sql("SPLIT([s], '$', -2)", wh=warehouse.BIGQUERY)
    assert "SAFE_OFFSET(3 - 1)" in to_sql("SPLIT([s], '$', 3)", wh=warehouse.BIGQUERY) or "SAFE_OFFSET(2)" in to_sql("SPLIT([s], '$', 3)", wh=warehouse.BIGQUERY)


def test_bigquery_text_operand_in_arithmetic_uses_safe_cast():
    from ossie_tableau.calc_parser import translate

    res = lambda ds, n: (warehouse_col(n), "str")  # noqa: E731

    def warehouse_col(n):
        from sqlglot import exp
        return exp.column(n)

    bq = translate("[a] / 3", res, warehouse.BIGQUERY)
    assert bq.sql == "SAFE_CAST(a AS FLOAT64) / 3" and any("SAFE_CAST" in n for n in bq.notes)
    assert translate("2024 - [a]", res, warehouse.BIGQUERY).sql == "2024 - SAFE_CAST(a AS FLOAT64)"
    assert translate("[a] + 'x'", res, warehouse.BIGQUERY).sql == "a || 'x'"
    assert translate("[a] / 3", res, warehouse.SNOWFLAKE).sql == "a / 3"


def test_bigquery_date_of_text_expression_tries_iso_then_us_format():
    from ossie_tableau.calc_parser import to_sql

    out = to_sql("DATE('12/31/' + STR([y]))", wh=warehouse.BIGQUERY)
    assert out.startswith("COALESCE(SAFE_CAST(") and "SAFE.PARSE_DATE('%m/%d/%Y'" in out
    assert "PARSE_DATETIME" in to_sql("DATETIME([s] + 'x')", wh=warehouse.BIGQUERY)
    assert "COALESCE" not in to_sql("DATE([d])", wh=warehouse.BIGQUERY)
    assert "COALESCE" not in to_sql("DATE('12/31/2015')", wh=warehouse.BIGQUERY)


def test_bigquery_ansi_quoted_column_is_a_physical_column_not_a_string_literal():
    doc = OssieDocument.model_validate({
        "name": "m",
        "datasets": [{"name": "T", "source": "p.d.T", "fields": [
            {"name": "Postal Code", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": '"Postal Code"'}]},
             "datatype": "Integer"}]}],
    })
    xml = write_tds(doc, BigQueryConn("p", "d")).xml
    assert "<remote-name>Postal Code</remote-name>" in xml
    assert "formula=\"'Postal Code'\"" not in xml


# --- Athena ---------------------------------------------------------------------------------------------------------

ATHENA_DOC = OssieDocument.model_validate({
    "name": "m",
    "datasets": [
        {"name": "orders", "source": "lakehouse.sales.orders", "primary_key": ["id"], "fields": [
            {"name": "id", "expression": {"sql": "id"}, "datatype": "Integer"},
            {"name": "customer_id", "expression": {"sql": "customer_id"}, "datatype": "Integer"},
            {"name": "amount", "expression": {"sql": "amount"}, "datatype": "Decimal"}]},
        {"name": "customers", "source": "sales.customers", "primary_key": ["id"], "fields": [
            {"name": "id", "expression": {"sql": "id"}, "datatype": "Integer"},
            {"name": "region", "expression": {"sql": "region"}, "datatype": "String"}]}],
    "relationships": [{"name": "orders_to_customers", "from": "orders", "to": "customers",
                       "from_columns": ["customer_id"], "to_columns": ["id"]}],
    "metrics": [
        {"name": "order_count", "expression": {"sql": "COUNT(orders.id)", "dialects": [{"dialect": "TRINO", "expression": "COUNT(orders.id)"}]}},
        {"name": "revenue", "expression": {"sql": "COUNT(orders.id)", "dialects": [{"dialect": "TRINO", "expression": "SUM(orders.amount)"}]}}],
})


def _athena():
    return AthenaConn("athena.example-region.amazonaws.com", "example_catalog", "sales", "s3://example-results/")


def test_athena_registry_and_lookup():
    assert warehouse.get("athena") is warehouse.ATHENA
    assert warehouse.by_tableau_class("athena") is warehouse.ATHENA
    assert warehouse.ATHENA.dialect_preference == ("TRINO", "ANSI_SQL", "OSSIE_SQL_2026")
    assert warehouse.ATHENA.sqlglot_dialect == "athena"
    assert warehouse.ATHENA.qualify_relations is False


def test_athena_conn_factory_builds_a_connection():
    assert _athena() == warehouse.Connection(warehouse.ATHENA, "athena.example-region.amazonaws.com", "example_catalog", "sales",
                                             "", "", {"warehouse": "s3://example-results/"})


def test_athena_connection_block():
    xml = write_tds(ATHENA_DOC, _athena()).xml
    node = ET.fromstring(xml).find(".//named-connection")
    assert node.get("name").startswith("athena.")
    assert dict(node.find("connection").attrib) == {
        "authentication": "auth-user-pass", "class": "athena", "dbname": "example_catalog", "one-time-sql": "", "port": "443",
        "schema": "sales", "server": "athena.example-region.amazonaws.com", "username": "", "warehouse": "s3://example-results/",
        "workgroup-auth-mode": ""}


def test_athena_relations_are_schema_and_table_with_the_catalog_from_the_connection():
    root = ET.fromstring(write_tds(ATHENA_DOC, _athena()).xml)
    tables = {r.get("name"): r.get("table") for r in root.iter("relation") if r.get("type") == "table"}
    # the 3-part source's first segment (a Spark/Glue catalog name) is replaced by the connection's dbname, and a
    # 2-part source is not prefixed with it
    assert tables == {"orders": "[sales].[orders]", "customers": "[sales].[customers]"}


def test_snowflake_still_qualifies_two_part_sources():
    root = ET.fromstring(write_tds(ATHENA_DOC, SnowflakeConn("a.snowflakecomputing.com", "WH", "DB", "SCH")).xml)
    tables = {r.get("table") for r in root.iter("relation") if r.get("type") == "table"}
    assert tables == {"[lakehouse].[sales].[orders]", "[DB].[sales].[customers]"}


def test_athena_metrics_prefer_the_trino_dialect_and_convert_without_loss():
    res = write_tds(ATHENA_DOC, _athena())
    assert [x.kind for x in res.losses] == []
    root = ET.fromstring(res.xml)
    formulas = {c.get("caption"): c.find("calculation").get("formula") for c in root.iter("column") if c.find("calculation") is not None}
    assert formulas == {"order_count": "COUNT([id])", "revenue": "SUM([amount])"}
    assert len(root.findall(".//object-graph/relationships/relationship")) == 1


def test_athena_cli_to_tds(tmp_path):
    import json
    import yaml
    src = tmp_path / "m.ossie.yaml"
    src.write_text(yaml.safe_dump(ATHENA_DOC.model_dump(mode="json", by_alias=True, exclude_none=True)))
    rc = main(["to-tds", str(src), "-o", str(tmp_path / "out"), "--warehouse-type", "athena",
               "--server", "athena.example-region.amazonaws.com", "--database", "example_catalog", "--schema", "sales",
               "--warehouse", "s3://example-results/"])
    assert rc == 0
    conn = ET.parse(tmp_path / "out" / "m.tds").getroot().find(".//named-connection/connection")
    assert (conn.get("class"), conn.get("dbname"), conn.get("schema"), conn.get("warehouse")) == (
        "athena", "example_catalog", "sales", "s3://example-results/")
    assert json.loads((tmp_path / "out" / "m.tds.loss.json").read_text()) == []


def test_athena_tds_reads_back_with_the_catalog_qualified_source_and_trino_sql(tmp_path):
    from ossie_tableau.convert import convert_file
    path = tmp_path / "m.tds"
    path.write_text(write_tds(ATHENA_DOC, _athena()).xml)
    [res] = convert_file(path)
    sources = {d.name: d.source for d in res.document.datasets}
    assert sources == {"orders": "example_catalog.sales.orders", "customers": "example_catalog.sales.customers"}
    dialects = {warehouse.dialect_name(d.dialect) for m in res.document.metrics for d in m.expression.dialects}
    assert "TRINO" in dialects
    # written again, the read-back model gives the same relations
    again = ET.fromstring(write_tds(res.document, _athena()).xml)
    assert {r.get("table") for r in again.iter("relation") if r.get("type") == "table"} == {"[sales].[orders]", "[sales].[customers]"}
