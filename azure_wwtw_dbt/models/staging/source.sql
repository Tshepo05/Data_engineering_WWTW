-- models/staging/source.sql
{{ config(materialized='view') }}

select
    "timestamp" as sample_ts,
    "F_in" as f_in,
    "C_in" as c_in,
    "F_in_pst_1" as f_in_pst_1,
    "C_in_pst_1" as c_in_pst_1,
    "F_out_pst_1" as f_out_pst_1,
    "C_out_pst_1" as c_out_pst_1,
    "C_pst_1" as c_pst_1,
    "F_underflow_pst_1" as f_underflow_pst_1,
    "F_in_ast_1" as f_in_ast_1,
    "C_in_ast_1" as c_in_ast_1,         
    "F_ox_in_ast_1" as f_ox_in_ast_1,
    "F_out_ast_1" as f_out_ast_1,
    "C_out_ast_1" as c_out_ast_1,
    "C_ast_1" as c_ast_1,
    "D_ast_1" as d_ast_1,               
    "F_ox_out_ast_1" as f_ox_out_ast_1,
    "F_in_sst_1" as f_in_sst_1,         
    "C_in_sst_1" as c_in_sst_1,         
    "F_out_sst_1" as f_out_sst_1,
    "C_out_sst_1" as c_out_sst_1,
    "C_sst_1" as c_sst_1,
    "F_underflow_sst_1" as f_underflow_sst_1 
from {{ ref('synthetic_wwtw_dataset_demo_2025') }}
