-- stg_visit_detail

{{ config(
    materialized='table',
	enabled=true)
}}

with episodes as (

    select * from {{ source('raw', 'episodes') }}
    WHERE "INGRES_UCI" = 'S'
    AND "DATA_ALTA"::timestamp >= "DATA_INGRES"::timestamp 
    AND "DATA_ALTA_UCI"::timestamp >= "HORA_INGRES_UCI"::timestamp

)

select
    {{ create_id_from_str('"EGA_id"::text || "HORA_INGRES_UCI"::text' )}} AS visit_detail_id, -- NOT NULL
    {{ create_id_from_str('"EGA_id"::text' )}} AS person_id, -- NOT NULL
    32037::int AS visit_detail_concept_id, -- Intensive care
    ep."HORA_INGRES_UCI"::date AS visit_detail_start_date, -- NOT NULL
    ep."HORA_INGRES_UCI"::timestamp AS visit_detail_start_datetime,
    ep."DATA_ALTA_UCI"::date AS visit_detail_end_date, -- NOT NULL
    ep."DATA_ALTA_UCI"::timestamp AS visit_detail_end_datetime,
    44818518::int AS visit_detail_type_concept_id, -- Derived from EHR source
    null::int AS provider_id,
    0::int AS care_site_id,
    null::varchar(50) AS visit_detail_source_value,
    0::int AS visit_detail_source_concept_id,
    0::int AS admitted_from_concept_id,
    null::varchar(50) AS admitted_from_source_value,
    null::varchar(50) AS discharged_to_source_value,
    0::int AS discharged_to_concept_id,
    null::int AS preceding_visit_detail_id,
    null::int AS parent_visit_detail_id,
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "DATA_ALTA"::text')}} AS visit_occurrence_id -- NOT NULL
from episodes ep 