{{ config(
    materialized='table',
	enabled=true
)
}}

with visit_diagnose AS (

    SELECT * FROM {{ ref('diagnoses_visit_occurrence') }}

),
with visit_procedure AS (

    SELECT * FROM {{ ref('procedures_visit_occurrence') }}

),
with visit_episode AS (

    SELECT * FROM {{ ref('episode_visit_occurrence') }}

),
-- All visit occurrence
with all_visits AS(
    
    SELECT * FROM visit_diagnose
    UNION
    SELECT * FROM visit_procedure
    UNION     
    SELECT * FROM visit_episode

)

SELECT *
FROM visit_episode
