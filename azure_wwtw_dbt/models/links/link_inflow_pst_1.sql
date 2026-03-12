{{ config(materialized='incremental')  }}

{% set source_model = "wwtw_blob_stg" %}

{% set src_pk = "INFLOW_PST_1_PK" %}
{% set src_fk = ["INFLOW_PK", "PST_1_PK"] %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}