{{ config(materialized='incremental')  }}

{% set source_model = "wwtw_blob_stg" %}

{% set src_pk = "PST_2_PK" %}
{% set src_hashdiff =  {"source_column": "WWTW_HASHDIFF",
                      "alias": "HASHDIFF"} %}
{% set src_payload = [
    "timestamp",
    "pst_2_inflow_rate",
    "pst_2_inflow_concentration",
    "pst_2_outflow_rate",
    "pst_2_outflow_concentration",
    "pst_2_concentration",
    "pst_2_underflow_rate"
] %}
{% set src_eff = "EFFECTIVE_FROM" %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                   src_eff=src_eff, src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model)   }}