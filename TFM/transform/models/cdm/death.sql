-- stg_death

{{ config(
   materialized='table',
   enabled=true
 )
}}

with episodes as (

    select * from {{ source('raw', 'episodes') }}
    WHERE "circ_alta_desc"::text = 'Defunció'

)

select
        {{ create_id_from_str('"EGA_id"::text' )}} as person_id, -- NOT NULL
        ep."DATA_ALTA_UCI"::date  as death_date, -- NOT NULL
        ep."DATA_ALTA_UCI"::timestamp as death_datetime,
        32817::integer as death_type_concept_id, -- EHR
        null::integer as cause_concept_id,
        null::text as cause_source_value,
        null::integer as cause_source_concept_id
from episodes ep