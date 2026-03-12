-- flowrate (F) and concentration (C) metrics across multiple stages of the wastewater treatment 
--process: primary sedimentation tanks (pst), aeration tanks (ast), secondary sedimentation tanks (sst),
-- and possibly oxidation stages
with source as (
    select * from {{ ref('source') }}
),

renamed as (
    select
        cast(sample_ts as timestamp) as sample_timestamp,
        cast(f_in as float) as inflow_rate,
        cast(c_in as float) as inflow_concentration,
        cast(f_in_pst_1 as float) as pst_inflow_rate,
        cast(c_in_pst_1 as float) as pst_inflow_concentration,
        cast(f_out_pst_1 as float) as pst_outflow_rate,
        cast(c_out_pst_1 as float) as pst_outflow_concentration,
        cast(c_pst_1 as float) as pst_concentration,
        cast(f_underflow_pst_1 as float) as pst_underflow_rate,
        cast(f_in_ast_1 as float) as ast_inflow_rate,
        cast(c_in_ast_1 as float) as ast_inflow_concentration,
        cast(f_ox_in_ast_1 as float) as ast_oxidation_inflow_rate,
        cast(f_out_ast_1 as float) as ast_outflow_rate,
        cast(c_out_ast_1 as float) as ast_outflow_concentration,
        cast(c_ast_1 as float) as ast_concentration,
        cast(d_ast_1 as float) as ast_dissolved_oxygen,
        cast(f_ox_out_ast_1 as float) as ast_oxidation_outflow_rate,
        cast(f_in_sst_1 as float) as sst_inflow_rate,
        cast(c_in_sst_1 as float) as sst_inflow_concentration,
        cast(f_out_sst_1 as float) as sst_outflow_rate,
        cast(c_out_sst_1 as float) as sst_outflow_concentration,
        cast(c_sst_1 as float) as sst_concentration,
        cast(f_underflow_sst_1 as float) as sst_underflow_rate
    from source
)

select * from renamed
