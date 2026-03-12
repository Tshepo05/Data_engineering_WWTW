-- flowrate (F) and concentration (C) metrics across multiple stages of the wastewater treatment 
--process: primary sedimentation tanks (pst), aeration tanks (ast), secondary sedimentation tanks (sst),
-- and possibly oxidation stages
{{ config(materialized='view') }}
with source as (
    select * from {{ ref('blob_ingest_raw') }}
),

blob_renamed as (
    select
       cast(id as INTEGER) as id,
       cast(source_file as text) as source_file,
       cast(inserted_at as date) as inserted_date,
       cast(blob_filename as text) as record_source ,
       cast("timestamp" as timestamp) as timestamp,
       cast(f_in as float) as inflow_rate,
       cast(c_in as float) as inflow_concentration,
       cast(id as INTEGER) as pst_1_id,
       cast(id as INTEGER) as pst_2_id,
       cast(f_in_pst_1 as float) as pst_1_inflow_rate,
       cast (f_in_pst_2 as float) as pst_2_inflow_rate,
       cast(c_in_pst_1 as float) as pst_1_inflow_concentration,
       cast(c_in_pst_2 as float)as pst_2_inflow_concentration,
       cast(f_out_pst_1 as float) as pst_1_outflow_rate,
       cast (f_out_pst_2 as float) as pst_2_outflow_rate,
       cast(c_out_pst_1 as float) as pst_1_outflow_concentration,
       cast(c_out_pst_2 as float) as pst_2_outflow_concentration,
       cast(c_pst_1 as float) as pst_1_concentration,
       cast(c_pst_2 as float) as pst_2_concentration,
       cast(f_underflow_pst_1 as float) as pst_1_underflow_rate,
       cast(f_underflow_pst_2 as float) as pst_2_underflow_rate,
       cast(id as INTEGER) as ast_id,
       cast(id as INTEGER) as ast_2_id,
       cast(f_in_ast_1 as float) as ast_1_inflow_rate,
       cast(f_in_ast_2 as float) as ast_2_inflow_rate,
       cast(c_in_ast_1 as float) as ast_1_inflow_concentration,
       cast(c_in_ast_2 as float) as ast_2_inflow_concentration,
       cast(f_ox_in_ast_1 as float) as ast_1_oxidation_inflow_rate,
       cast(f_ox_in_ast_2 as float) as ast_2_oxidation_inflow_rate,
       cast(f_out_ast_1 as float) as ast_1_outflow_rate,
       cast(f_out_ast_2 as float) as ast_2_outflow_rate,
       cast(c_out_ast_1 as float) as ast_1_outflow_concentration,
       cast(c_out_ast_2 as float) as ast_2_outflow_concentration,
       cast(c_ast_1 as float) as ast_1_concentration,
       cast(c_ast_2 as float) as ast_2_concentration,
       cast(d_ast_1 as float) as ast_1_dissolved_oxygen,
       cast(d_ast_2 as float) as ast_2_dissolved_oxygen,
       cast(f_ox_out_ast_1 as float) as ast_1_oxidation_outflow_rate,
       cast(f_ox_out_ast_2 as float) as ast_2_oxidation_outflow_rate,
       cast(id as INTEGER) as sst_id,
       cast(f_in_sst_1 as float) as sst_1_inflow_rate,
       cast(c_in_sst_1 as float) as sst_1_inflow_concentration,
       cast(f_out_sst_1 as float) as sst_1_outflow_rate,
       cast(c_out_sst_1 as float) as sst_1_outflow_concentration,
       cast(c_sst_1 as float) as sst_1_concentration,
       cast(f_underflow_sst_1 as float) as sst_1_underflow_rate,
       cast (t as int) as t

    from source
)
select * from blob_renamed