-- person

{{ config(
    materialized='table',
	enabled=true
)
}}

with patients as (

    select *, row_number() over (partition by "EGA_id" order by "Age" desc) rn
    from {{ source('raw', 'patient') }}

),
person as (
    select
        {{ create_id_from_str('"EGA_id"::text')}} AS person_id,
        {{ gender_concept_id ('"Sex"') }} AS gender_concept_id,
        {{calculate_year('"Age"::int')}}::INT AS year_of_birth,
        NULL::INT AS month_of_birth,
        NULL::INT AS day_of_birth,
        NULL::TIMESTAMP AS birth_datetime,
        0::INT AS race_concept_id,
        0::INT AS ethnicity_concept_id,
        NULL::INT AS location_id,
        NULL::INT AS provider_id,
        NULL::INT AS care_site_id,
        "EGA_id"::VARCHAR(50) AS person_source_value,
        "Sex"::VARCHAR(50) AS gender_source_value,
        0::INT AS gender_source_concept_id,
        NULL::VARCHAR(50) AS race_source_value,
        0::INT AS race_source_concept_id,
        NULL::VARCHAR(50) AS ethnicity_source_value,
        0::INT AS ethnicity_source_concept_id
    from patients
    where "Age" is not null -- Don't load patients who do not have birthdate and sex
    and "Sex" is not null
    and rn = 1
)
select * from person
 