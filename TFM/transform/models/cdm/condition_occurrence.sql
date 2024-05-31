-- stg_condition_occurrence

{{ config(
   materialized='table',
   enabled=false
)
}}

WITH conditions AS (

    SELECT * FROM {{ source('raw', 'diagnoses') }}

),
source_to_standard_vocab_map AS (

    SELECT * FROM {{ ref('source_to_standard_vocab_map') }}
    WHERE source_vocabulary_id = 'SNOMED'
        AND source_domain_id = 'Condition'
        AND target_domain_id = 'Condition'
        AND target_standard_concept = 'S'
        AND target_invalid_reason IS NULL

),
source_to_source_vocab_map AS (

    SELECT * FROM {{ ref('source_to_source_vocab_map') }}
    WHERE source_vocabulary_id = 'SNOMED'

),
final_visit_ids AS (

    SELECT * FROM {{ ref('final_visit_ids') }}

),
person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "pos_dx"::text ')}} AS condition_occurrence_id,
    p.person_id AS person_id,
    -- case when srctostdvm.target_concept_id is NULL then 0 else srctostdvm.target_concept_id end AS condition_concept_id,
    c."DATA_INGRES"::DATE AS condition_start_date,
    c."DATA_INGRES"::TIMESTAMP AS condition_start_datetime,
    NULL::DATE AS condition_end_date,
    NULL::TIMESTAMP AS condition_end_datetime,
    32020::int AS condition_type_concept_id, -- EHR encounter diagnosis
    {{ condition_status_concept_id ('"pos_dx"') }} AS condition_status_concept_id, 
    NULL::varchar(20) AS stop_reason,
    NULL::int AS provider_id,
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text ')}} AS visit_occurrence_id,
    NULL::bigint AS visit_detail_id,
    c."ICD10"::varchar(50) AS condition_source_value,
    -- case when srctosrcvm.target_concept_id is NULL then 0 else srctosrcvm.target_concept_id end AS condition_source_concept_id,
    c."pos_dx"::varchar(50) AS condition_status_source_value
from conditions c
left join source_to_standard_vocab_map srctostdvm
    on srctostdvm.source_code = c.code
left join source_to_source_vocab_map srctosrcvm
    on srctosrcvm.source_code = c.code
join final_visit_ids fv
    on fv.encounter_id = c.encounter
join person p on c.patient = p.person_source_value