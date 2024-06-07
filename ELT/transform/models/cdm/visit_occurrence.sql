{{ config(
    materialized='table',
	enabled=true
)
}}

with visit_diagnose AS (

    SELECT * FROM {{ ref('diagnoses_visit_occurrence') }}

),
visit_procedure AS (

    SELECT * FROM {{ ref('procedures_visit_occurrence') }}

),
visit_episode AS (

    SELECT * FROM {{ ref('episode_visit_occurrence') }}

),
visit_external_cause AS (

    SELECT * FROM {{ ref('external_cause_visit_occurrence') }}

),
-- All visit occurrence
all_visits AS(
    
    SELECT * FROM visit_diagnose
    UNION
    SELECT * FROM visit_procedure
    UNION     
    SELECT * FROM visit_episode
    UNION
    SELECT * FROM visit_external_cause

)

SELECT *
FROM all_visits
