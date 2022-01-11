import os
from dotenv import find_dotenv, load_dotenv
import asyncpg

from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .database import postgis_query_to_geojson, sql_query_raw


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


@app.get(URL_PREFIX + "/flows/", tags=["flows"])
async def get_flows(dest_name: str = Query(None)):
    """
    For a given destination name:
        - see if the table has already been computed
        - if so, return the pre-computed result quickly
        - if not, compute it and then return the result
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

    dest_name_clean = (
        dest_name.replace(" ", "_").replace("-", "_").replace(r"/", "_").replace(r"\\", "_").lower()
    )

    if "d_" + dest_name_clean not in computed_tables:
        compute_table_query = f"""
            create table computed.d_{dest_name_clean} as

            with trips as (
                select origzoneno, sum(odtrips) as odtrips
                from existing_2019am_rr_to_dest_zone_fullpath
                where destzoneno in (
                    select tazt::int from zones
                    where zone_name = '{dest_name}'
                )
                and pathlegindex = '0'
                group by origzoneno
            ),
            joined_data as (
                select
                    s.tazt,
                    st_transform(s.geom, 4326) as geometry,
                    t.odtrips as total_trips,
                    st_area(s.geom) as shape_area,
                    t.odtrips / st_area(s.geom) as trip_density
                from data.taz_2010 as s
                inner join trips t on s.tazt::int = t.origzoneno
            )
            select 
                j.*,
                s.pct_non_english, s.bucket_pct_non_english
            from joined_data j
            left join ctpp.summary s on j.tazt = s.taz_id::text
         """

        conn = await asyncpg.connect(DATABASE_URL)

        await conn.execute(compute_table_query)

        await conn.close()

    query = f"""
        SELECT * FROM computed.d_{dest_name_clean}
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
    For a given destination name, summarize the results by demographic bucket
    """

    dest_name_clean = (
        dest_name.replace(" ", "_").replace("-", "_").replace(r"/", "_").replace(r"\\", "_").lower()
    )

    sql_tablename = f"computed.d_{dest_name_clean}"

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
async def define_new_group_of_tazs(new_zone: NewZone):
    """
    Add one or many new rows to the 'zones' table.
    This table defines which TAZs belong to a given 'destination',
    which is comprised of a group of TAZs.

    """

    zone_name = new_zone.zone_name

    values = [(zone_name, str(tazid)) for tazid in new_zone.tazt]

    conn = await asyncpg.connect(DATABASE_URL)

    await conn.executemany(
        """
        INSERT INTO public.zones(zone_name, tazt) VALUES($1, $2)
    """,
        values,
    )

    await conn.close()

    return {"data": new_zone}
