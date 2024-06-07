-- stg_observation

{{ config(
   materialized='table',
   enabled=true
)
}}


WITH external AS (

    SELECT * FROM {{ source('raw', 'external_cause') }}

),
person AS (

    SELECT * FROM {{ ref('person') }}

)

SELECT
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text || "posicio"::text ')}} AS observation_id,
    p.person_id AS person_id,
    c2.concept_id AS observation_concept_id,
    ec."DATA_INGRES"::DATE AS observation_date,
    ec."DATA_INGRES"::TIMESTAMP  AS observation_datetime,
    32020 AS observation_type_concept_id, -- EHR encounter diagnosis
    null::float AS value_as_number,
    null::varchar AS value_as_string,
    0::int AS value_as_concept_id,
    0::int AS qualifier_concept_id,
    0::int AS unit_concept_id,
    null::bigint AS provider_id,
    {{ create_id_from_str('"EGA_id"::text || "DATA_INGRES"::text ')}} AS visit_occurrence_id,
    null::bigint AS visit_detail_id,
    ec."codi"::varchar(50) AS observation_source_value,
    concept.concept_id AS observation_source_concept_id,
    null::varchar AS unit_source_value,
    null::varchar AS qualifier_source_value,
    null::varchar AS value_source_value,
    null::bigint AS observation_event_id,
    0::int AS obs_event_field_concept_id
from external ec
join {{ source('vocabularies', 'concept') }} concept on ec."codi" = concept.concept_code
join {{ source('vocabularies', 'concept_relationship') }} as cr 
  on cr.concept_id_1 = concept.concept_id and lower(cr.relationship_id) = 'maps to'
join {{ source('vocabularies', 'concept') }} c2 on c2.concept_id = cr.concept_id_2 and c2.domain_id = 'Observation' 
join person p on ec."EGA_id" = p.person_source_value