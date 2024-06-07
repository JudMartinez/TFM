--------------------------
--    ~~ GENERAL ~~     --
--------------------------

-- Macro to create a bigint type ID from a string in absolute value
{% macro create_id_from_str(text) %}
    abs(('x' || substr(md5({{ text }}), 1, 16))::bit(64)::bigint)
{% endmacro %}

-- OMOP TABLE: person
--- Macro to transform 'M' and 'F' sex values into their concept_id
{% macro gender_concept_id(sex) %}
(CASE WHEN {{ sex }} = 'Male' THEN 8507::int -- Male
      WHEN {{ sex }} = 'Female' THEN 8532::int -- Female
      WHEN {{ sex }} is null THEN 0::int -- No data
      ELSE 8551::int -- Unknown
      END)
{% endmacro %}

-- OMOP TABLE: person
--- Macro to calculate the year of birth from age
{%macro calculate_year(age) %}
(2014 - {{age}})
{% endmacro %}


-- OMOP TABLE: visit_occurrence
-- Macro to transform encounter class values into their concept_id
{% macro visit_concept_id(encounter_class) %}
(CASE {{ encounter_class }}
        WHEN 'ambulatory' THEN 9202 -- Outpatient Visit
        WHEN 'emergency' THEN 9203 -- Emergency Room Visit
        WHEN 'inpatient' THEN 9201 -- Inpatient Visit
        WHEN 'wellness' THEN 9202 -- Outpatient Visit
        WHEN 'urgentcare' THEN 9203 -- Emergency Room Visit
        WHEN 'outpatient' THEN 9202 -- Outpatient Visit
        ELSE 0
        END)
{% endmacro %}


-- OMOP TABLE: condition_occurrence
-- Macro to transform condition_status values into their concept_id
{% macro condition_status_concept_id(pos_dx) %}
(CASE WHEN {{ pos_dx }} = '1' THEN 32902::int -- Primary diagnosis
      WHEN {{ pos_dx }} = '2' THEN 32908::int -- Secondary diagnosis
      ELSE 0::int -- Unknown
      END)
{% endmacro %}