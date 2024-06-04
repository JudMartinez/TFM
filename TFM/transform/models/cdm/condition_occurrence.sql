-- stg_condition_occurrence

{{ config(
   materialized='table',
   enabled=true
)
}}

with condition_diagnose AS (

    SELECT * FROM {{ ref('diagnoses_condition_occurrence') }}

),
condition_external_cause AS (

    SELECT * FROM {{ ref('external_cause_condition_occurrence') }}

),

-- All visit occurrence
all_conditions AS(
    
    SELECT * FROM condition_diagnose
    UNION
    SELECT * FROM condition_external_cause

)

SELECT *
FROM all_conditions
