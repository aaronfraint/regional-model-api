import os
from dotenv import find_dotenv, load_dotenv
import asyncpg
from asyncio import sleep
from typing import List
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .database import postgis_query_to_geojson, sql_query_raw
from .queries import SQL_COMPUTE_ZONES

load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("DATABASE_URL", None)
URL_PREFIX = os.getenv("URL_PREFIX", "")

app = FastAPI(docs_url=URL_PREFIX)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class NewZone(BaseModel):
    zone_name: str
    tazt: List[str]


@app.get(URL_PREFIX + "/zone-names", tags=["zones"])
async def zone_names_with_list_of_taz_ids():
    """
    This route provides the name of each zone
    """
    query = """
        select distinct zone_name from zones
    """
    return await sql_query_raw(query, DATABASE_URL)


@app.get(URL_PREFIX + "/zone-geoms", tags=["zones"])
async def zone_shapes():
    """
    This route provides a geojson of TAZ groups
    """

    query = """
        select zone_name, geom as geometry from zone_shapes
    """
    return await postgis_query_to_geojson(query, ["zone_name", "geometry"], DATABASE_URL)


def turn_zone_name_into_sql_string(zone_name: str) -> str:

    sql_name = zone_name.lower()

    for char in [
        " ",
        "-",
        r"/",
        r"\\",
        "(",
        ")",
    ]:
        sql_name = sql_name.replace(char, "_")

    return sql_name


async def compute_zone_table(zone_name: str) -> None:
    """
    Use the SQL_COMPUTE_ZONES template to generate a
    summary table for the provided `zone_name`.
    """

    sql_name = turn_zone_name_into_sql_string(zone_name)

    compute_table_query = SQL_COMPUTE_ZONES.replace(
        "NEW_TABLENAME", f"computed.d_{sql_name}"
    ).replace("ZONE_NAME_STRING", zone_name)

    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute(compute_table_query)
    await conn.close()


async def the_table_does_not_exist(zone_name: str):
    """
    Confirm if a given `zone_name` has finished computing
    """

    computed_tables_query = await sql_query_raw(
        f"""
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'computed'
    """,
        DATABASE_URL,
    )

    computed_tables = [x[0] for x in computed_tables_query]

    sql_name = turn_zone_name_into_sql_string(zone_name)

    return f"d_{sql_name}" not in computed_tables


@app.get(URL_PREFIX + "/flows/", tags=["flows"])
async def get_flows(dest_name: str = Query(None)):
    """
    For a given destination name, wait until the
    summary table has been computed and then return
    it as a geojson
    """

    # wait until the computed table exists
    while await the_table_does_not_exist(dest_name):
        await sleep(1)

    sql_name = turn_zone_name_into_sql_string(dest_name)

    query = f"""
        SELECT * FROM computed.d_{sql_name}
    """

    return await postgis_query_to_geojson(
        query,
        [
            "tazt",
            "geometry",
            "total_trips",
            "shape_area",
            "trip_density",
            "pct_non_english",
            "bucket_pct_non_english",
        ],
        DATABASE_URL,
    )


@app.get(URL_PREFIX + "/demographic-flows/", tags=["flows"])
async def get_flows_by_demographic(
    dest_name: str = Query(None),
    demo_type: str = Query("bucket_pct_non_english"),
    metric_column: str = Query("total_trips"),
):
    """
    For a given destination name, wait until the summary table
    is computed and then summarize the results by demographic bucket
    """

    # wait until the computed table exists
    while await the_table_does_not_exist(dest_name):
        await sleep(1)

    sql_name = turn_zone_name_into_sql_string(dest_name)

    sql_tablename = f"computed.d_{sql_name}"

    query = f"""
        select
            {demo_type},
            sum({metric_column}) as trip_sum
        from {sql_tablename}
        group by {demo_type}
        order by {demo_type}
    """

    return await sql_query_raw(
        query,
        DATABASE_URL,
    )


@app.post(URL_PREFIX + "/new-taz-group/", tags=["zones"])
async def define_new_group_of_tazs(new_zone: NewZone, background_tasks: BackgroundTasks):
    """
    Add one or many new rows to the 'zones' table.
    This table defines which TAZs belong to a given 'destination',
    which is comprised of a group of TAZs.

    """

    # prepare the data to save to db as a list of tuples
    zone_name = new_zone.zone_name
    values = [(zone_name, str(tazid)) for tazid in new_zone.tazt]

    # insert a row for each selected TAZ
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.executemany(
        """
        INSERT INTO public.zones(zone_name, tazt) VALUES($1, $2)
    """,
        values,
    )
    await conn.close()

    # kick off the background process of computing the table for this zone
    background_tasks.add_task(compute_zone_table, zone_name)

    return {"data": new_zone}
