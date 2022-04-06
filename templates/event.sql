{{config(materialized='view')}}

SELECT
    '[[ .ContractAddress ]]' as contract_address
    ,log_index as evt_index
    ,block_number as evt_block_number
    ,transaction_hash as evt_tx_hash
    [[ range .Inputs ]]
    ,f_decode_abi_input_parameter(substring([[ .ColumnName ]], [[ .StartPos ]], 64), '[[ .Type ]]') AS inp_[[ .Name ]]
    [[ end ]]
FROM {{ source(env_var('DBT_SF_SCHEMA'), 'logs') }}
WHERE address = '[[ .ContractAddress ]]' AND substring(topics, 1, 66) = '[[ .SigHash ]]'