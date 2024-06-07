-- stg_procedure_occurrence

{{ config(
   materialized='table',
   enabled=true
)
}}

with procedures as (

    select * from {{ source('raw', 'procedures') }}

),

person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "pos_proc"::text ') }} AS procedure_occurrence_id,
    p.person_id AS person_id,
    concept.concept_id AS procedure_concept_id,
    pr."DATA_INGRES"::DATE AS procedure_date,
    pr."DATA_INGRES"::timestamp AS procedure_datetime,
    NULL::date AS procedure_end_date,
    NULL::timestamp AS procedure_end_datetime,
    38000275 AS procedure_type_concept_id, -- EHR order list entry
    0 AS modifier_concept_id,
    null::int AS quantity,
    null::bigint AS provider_id,
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text ')}} AS visit_occurrence_id,
    null::bigint AS visit_detail_id,
    pr."ICD10"::Varchar(50) AS procedure_source_value,
    concept.concept_id AS procedure_source_concept_id,
    null::varchar(50) AS modifier_source_value
from procedures pr
join {{ source('vocabularies', 'concept') }} concept on pr."ICD10" = concept.concept_code
join person p on pr."EGA_id" = p.person_source_value