
{{ config(materialized='view')}}

{% set yaml_metadata %}

source_model: stg_blob_ingest
derived_columns:
  RECORD_SOURCE: record_source
  LOAD_DATE: "CAST('{{ run_started_at }}' AS TIMESTAMP)"
  EFFECTIVE_FROM: inserted_date
hashed_columns:
  INFLOW_PK: id
  PST_1_PK: pst_1_id
  PST_2_PK: pst_2_id
  AST_1_PK: ast_id
  AST_2_PK: ast_2_id
  SST_1_PK: sst_id
  INFLOW_PST_1_PK: 
    - id
    - pst_1_id
  INFLOW_PST_2_PK: 
    - id
    - pst_2_id
  PST_1_AST_1_PK:
    - pst_1_id
    - ast_id
  PST_2_AST_2_PK:
    - pst_2_id
    - ast_2_id
  AST_1_SST_1_PK:
    - ast_id
    - sst_id
  AST_2_SST_1_PK:
    - ast_2_id
    - sst_id
  WWTW_HASHDIFF:
    - id
    - pst_1_id
    - pst_2_id
    - ast_id
    - ast_2_id
    - sst_id
    - timestamp
    - inflow_rate
    - inflow_concentration
    - pst_1_inflow_rate
    - pst_2_inflow_rate
    - pst_1_inflow_concentration
    - pst_2_inflow_concentration
    - pst_1_outflow_rate
    - pst_2_outflow_rate
    - pst_1_outflow_concentration
    - pst_2_outflow_concentration
    - pst_1_concentration
    - pst_2_concentration
    - pst_1_underflow_rate
    - pst_2_underflow_rate
    - ast_1_inflow_rate
    - ast_2_inflow_rate
    - ast_1_inflow_concentration
    - ast_2_inflow_concentration
    - ast_1_oxidation_inflow_rate
    - ast_2_oxidation_inflow_rate
    - ast_1_outflow_rate
    - ast_2_outflow_rate
    - ast_1_outflow_concentration
    - ast_2_outflow_concentration
    - ast_1_concentration
    - ast_2_concentration
    - ast_1_dissolved_oxygen
    - ast_2_dissolved_oxygen
    - ast_1_oxidation_outflow_rate
    - ast_2_oxidation_outflow_rate
    - sst_1_inflow_rate
    - sst_1_inflow_concentration
    - sst_1_outflow_rate
    - sst_1_outflow_concentration
    - sst_1_concentration
    - sst_1_underflow_rate
    - t
    - EFFECTIVE_FROM
{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns)}}
                   



