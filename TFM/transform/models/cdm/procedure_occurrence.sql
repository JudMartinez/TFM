-- stg_procedure_occurrence

{{ config(
   materialized='table',
   enabled=false
)
}}

with procedures as (

    select * from {{ source('raw', 'procedures') }}

),

source_to_standard_vocab_map AS (

    SELECT * FROM {{ ref('source_to_standard_vocab_map') }}
    WHERE source_vocabulary_id = 'SNOMED'
        AND source_domain_id = 'Procedure'
        AND target_domain_id = 'Procedure'
        AND target_standard_concept = 'S'
        AND target_invalid_reason IS NULL

),
source_to_source_vocab_map AS (

    SELECT * FROM {{ ref('source_to_source_vocab_map') }}
    WHERE source_vocabulary_id  = 'SNOMED'

),

person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "pos_proc"::text ') }} AS procedure_occurrence_id,
    p.person_id AS person_id,
    -- case when srctostdvm.target_concept_id is NULL then 0 else srctostdvm.target_concept_id end AS procedure_concept_id,
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
    pr.code AS procedure_source_value,
    case when srctosrcvm.target_concept_id is NULL then 0 else srctosrcvm.target_concept_id end AS procedure_source_concept_id,
    null::varchar(50) AS modifier_source_value
from procedures pr
left join source_to_standard_vocab_map srctostdvm
    on srctostdvm.source_code = pr.code
left join source_to_source_vocab_map srctosrcvm
    on srctosrcvm.source_code = pr.code
join final_visit_ids fv
    on fv.encounter_id = pr.encounter
join person p
  on p.person_source_value = pr.patient