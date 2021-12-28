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


@app.get(URL_PREFIX + "/zone-names")
async def zone_names_with_list_of_taz_ids():
    """
    This route provides the name of each zone
    """
    query = """
        select distinct zone_name from zones
    """
    return await sql_query_raw(query, DATABASE_URL)


@app.get(URL_PREFIX + "/zone-geoms")
async def zone_shapes():
    """
    This route provides a geojson of TAZ groups
    """

    query = """
        select zone_name, geom as geometry from zone_shapes
    """
    return await postgis_query_to_geojson(query, ["zone_name", "geometry"], DATABASE_URL)


@app.get(URL_PREFIX + "/flows/")
async def get_flows(q: List[int] = Query(None)):
    if len(q) == 1:
        queried_ids = f"({q[0]})"
    else:
        queried_ids = tuple(q)

    query = f"""
        with trips as (
            select destzoneno, sum(odtrips) as odtrips
            from existing_2019am_rr_to_dest_zone_fullpath
            where origzoneno in {queried_ids} and pathlegindex = '0'
            group by destzoneno
        )
        select
            s.tazt,
            st_transform(s.geom, 4326) as geometry,
            t.odtrips as total_trips,
            st_area(s.geom) as shape_area,
            t.odtrips / st_area(s.geom) as trip_density
        from data.taz_2010 as s
        inner join trips t on s.tazt = t.destzoneno::text
    """

    return await postgis_query_to_geojson(
        query,
        ["tazt", "geometry", "total_trips", "shape_area", "trip_density"],
        DATABASE_URL,
    )


@app.post(URL_PREFIX + "/new-taz-group/")
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
