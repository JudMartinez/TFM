-- stg_condition_occurrence

{{ config(
   materialized='table',
   enabled=true,
   schema='staging'
)
}}

WITH external AS (

    SELECT * FROM {{ source('raw', 'external_cause') }}

),

person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "posicio"::text ')}} AS condition_occurrence_id,
    p.person_id AS person_id,
    c2.concept_id AS condition_concept_id,
    ec."DATA_INGRES"::DATE AS condition_start_date,
    ec."DATA_INGRES"::TIMESTAMP AS condition_start_datetime,
    NULL::DATE AS condition_end_date,
    NULL::TIMESTAMP AS condition_end_datetime,
    32020::int AS condition_type_concept_id, -- EHR encounter diagnosis
    {{ condition_status_concept_id ('"posicio"') }} AS condition_status_concept_id, 
    NULL::varchar(20) AS stop_reason,
    NULL::int AS provider_id,
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text ')}} AS visit_occurrence_id,
    NULL::bigint AS visit_detail_id,
    ec."codi"::varchar(50) AS condition_source_value,
    concept.concept_id AS condition_source_concept_id,
    ec."posicio"::varchar(50) AS condition_status_source_value
from external ec
join {{ source('vocabularies', 'concept') }} concept on ec."codi" = concept.concept_code
join {{ source('vocabularies', 'concept_relationship') }} as cr 
  on cr.concept_id_1 = concept.concept_id and lower(cr.relationship_id) = 'maps to'
join {{ source('vocabularies', 'concept') }} c2 on c2.concept_id = cr.concept_id_2 and c2.domain_id = 'Condition' 
join person p on ec."EGA_id" = p.person_source_value
