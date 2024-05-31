-- procedures_visit_occurrence

{{ config(
   materialized='table',
   enabled=true,
   schema='staging'
)
}}

with procedures as (

    select *
    from {{ source('raw', 'procedures') }}

),
person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text ')}} AS visit_occurrence_id,
    p.person_id AS person_id,
    {{ visit_concept_id("'inpatient'") }} AS visit_concept_id,
    pr."DATA_INGRES"::DATE AS visit_start_date,
    pr."DATA_INGRES"::TIMESTAMP AS visit_start_datetime,
    NULL::DATE AS visit_end_date,
    NULL::TIMESTAMP AS visit_end_datetime,
    44818518::int AS visit_type_concept_id, -- Visit derived from EHR record
    NULL::int AS provider_id,
    NULL::int AS care_site_id,
    NULL::varchar(50) AS visit_source_value,
    0::int AS visit_source_concept_id,
    0::int AS admitted_from_concept_id,
    NULL::int AS admitted_from_source_value,
    NULL::int AS discharged_to_concept_id, -- Need to check this
    NULL::varchar(50) AS discharged_to_source_value, 
    0::int AS preceding_visit_occurrence_id
FROM procedures pr
JOIN person p ON pr."EGA_id" = p.person_source_value