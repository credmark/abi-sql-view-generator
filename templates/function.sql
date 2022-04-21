CREATE OR REPLACE VIEW ethereum_contracts.{{ .Namespace }}_{{ .ContractAddress }}_fn_{{ .Name }}
    AS
        WITH q1 AS (
            SELECT
                '{{ .ContractAddress }}' as contract_address
                ,hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                ,null as error
                ,input
            FROM ethereum.transactions
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'

            UNION

            SELECT 
                '{{ .ContractAddress }}' as contract_address
                ,transaction_hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                ,error
                ,input
            FROM ethereum.traces
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'
        )

        ,q2 AS (
            SELECT 
                row_number() OVER (PARTITION BY contract_address, txn_hash, txn_block_number, txn_index ORDER BY error) as row_num
                ,*
                ,CASE WHEN error IS NULL THEN true ELSE false END AS is_successful_txn
            FROM q1
        )

        ,q3 AS (
            SELECT
                *
                ,ethereum_contracts.decode_abi_input_prod(input, '', parse_json('{{ .InputsJson }}'), 'method', is_successful_txn) AS val
            FROM q2
            WHERE row_num = 1
        )

        SELECT
            contract_address
            ,txn_block_number
            ,txn_hash
            ,txn_index
            ,is_successful_txn
            {{ range .Inputs }}
            ,val:{{ .Name }} as inp_{{ .Name }}
            {{ end }}
        FROM q3
        ORDER BY txn_block_number, txn_index;
