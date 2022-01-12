SQL_COMPUTE_ZONES = """
    create table NEW_TABLENAME as

    with trips as (
        select origzoneno, sum(odtrips) as odtrips
        from existing_2019am_rr_to_dest_zone_fullpath
        where destzoneno in (
            select tazt::int from zones
            where zone_name = 'ZONE_NAME_STRING'
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
