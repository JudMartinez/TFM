-- cdm_source

{{ config(
   materialized='table',
   enabled=true
 )
}}


select
  'datoscat_tfm'::varchar(255) as cdm_source_name, -- NOT NULL
  'datoscat_tfm'::varchar(25) as cdm_source_abbreviation,
  'BSC'::varchar(255) as cdm_holder,
  'datoscat_tfm'::text as source_description,
  null::varchar(255) as source_documentation_reference,
  null::varchar(255) as cdm_etl_reference,
  null::date as source_release_date,
  null::date as cdm_release_date,
  'CDM v5.4'::varchar(10) as cdm_version,
  null::varchar(20) as vocabulary_version