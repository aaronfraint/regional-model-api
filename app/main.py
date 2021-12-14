import os
from dotenv import find_dotenv, load_dotenv

from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from .database import postgis_query_to_geojson, sql_query_raw


load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("DATABASE_URL", None)
URL_PREFIX = os.getenv("URL_PREFIX", "")

app = FastAPI(docs_url=URL_PREFIX)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# @app.get(URL_PREFIX + "/zone-ids")
# async def zone_names_with_list_of_taz_ids():
#     """
#     This route provides the name of each zone,
#     along with a list of TAZ IDs that belong
#     to that zone
#     """
#     query = """
#         select
#             zone_name,
#             array_agg(tazt) as taz_list
#         from
#             zones
#         group by
#             zone_name
#     """
#     return await sql_query_raw(query, DATABASE_URL)


# @app.get(URL_PREFIX + "/zone-geoms")
# async def zone_shapes():
#     """
#     This route provides a geojson of TAZ shapes with ID
#     """
#     query = """
#         select
#             tazt,
#             st_transform(geom, 4326) as geometry
#         from
#             taz_2010
#     """
#     return await postgis_query_to_geojson(query, ["tazt", "geometry"], DATABASE_URL)


@app.get(URL_PREFIX + "/flows/")
async def get_flows(q: List[int] = Query(None)):
    if len(q) == 1:
        queried_ids = f"({q[0]})"
    else:
        queried_ids = tuple(q)

    query = f"""
        with trips as (
            select tozone, sum(mat2150) as mat2150
            from data.existing_od_transit_auto
            where fromzone::int in {queried_ids}
            group by tozone
        )
        select
            s.tazt,
            st_transform(s.geom, 4326) as geometry,
            t.mat2150 as total_trips,
            st_area(s.geom) as shape_area,
            t.mat2150 / st_area(s.geom) as trip_density
        from data.taz_2010 as s
        inner join trips t on s.tazt = t.tozone
        order by mat2150 desc
    """

    return await postgis_query_to_geojson(
        query,
        ["tazt", "geometry", "total_trips", "shape_area", "trip_density"],
        DATABASE_URL,
    )
