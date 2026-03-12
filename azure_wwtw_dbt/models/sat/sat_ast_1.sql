{{ config(materialized='incremental')  }}

{% set source_model = "wwtw_blob_stg" %}

{% set src_pk = "AST_1_PK" %}
{% set src_hashdiff =  {"source_column": "WWTW_HASHDIFF",
                      "alias": "HASHDIFF"} %}
{% set src_payload = [
    "timestamp",
    "ast_1_inflow_rate",
    "ast_1_inflow_concentration",
    "ast_1_oxidation_inflow_rate",
    "ast_1_outflow_rate",
    "ast_1_outflow_concentration",
    "ast_1_concentration",
    "ast_1_dissolved_oxygen",
    "ast_1_oxidation_outflow_rate"
] %}
{% set src_eff = "EFFECTIVE_FROM" %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                   src_eff=src_eff, src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model)   }}