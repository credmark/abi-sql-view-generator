{{config(materialized='view')}}

SELECT
    '[[ .ContractAddress ]]' as contract_address
    ,hash as txn_hash
    ,block_number as txn_block_number
    ,transaction_index as txn_index
    [[ range .Inputs ]]
    ,f_decode_abi_input_parameter(substring([[ .ColumnName ]], [[ .StartPos ]], 64), '[[ .Type ]]') AS inp_[[ .Name ]]
    [[ end ]]
FROM {{ source(env_var('DBT_SF_SCHEMA'), 'transactions') }}
WHERE to_address='[[ .ContractAddress ]]' AND substring(input, 1, 10)='[[ .MethodId ]]'

UNION

SELECT 
    '[[ .ContractAddress ]]' as contract_address
    ,transaction_hash as txn_hash
    ,block_number as txn_block_number
    ,transaction_index as txn_index
    [[ range .Inputs ]]
    ,f_decode_abi_input_parameter(substring([[ .ColumnName ]], [[ .StartPos ]], 64), '[[ .Type ]]') AS inp_[[ .Name ]]
    [[ end ]]
FROM {{ source(env_var('DBT_SF_SCHEMA'), 'traces') }}
WHERE to_address='[[ .ContractAddress ]]' AND substring(input, 1, 10)='[[ .MethodId ]]'