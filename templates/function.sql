CREATE OR REPLACE VIEW ethereum_contracts.{{ .Namespace }}_{{ .ContractAddress }}_fn_{{ .Name }}
    AS
        WITH UNIONED AS (
            SELECT
                '{{ .ContractAddress }}' as contract_address
                ,hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                ,ethereum_contracts.decode_abi_inputs_prod(input, '', parse_json('{{ .InputsJson }}'), "method") AS val
            FROM ethereum.transactions
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'

            UNION

            SELECT 
                '{{ .ContractAddress }}' as contract_address
                ,transaction_hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                ,ethereum_contracts.decode_abi_inputs_prod(input, '', parse_json('{{ .InputsJson }}'), "method") AS val
            FROM ethereum.traces
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'
        )

        SELECT
            contract_address
            ,txn_block_number
            ,txn_hash
            ,txn_index
            {{ range .Inputs }}
            ,val:{{ .Name }} as inp_{{ .Name }}
            {{ end }}
        FROM UNIONED
        ORDER BY txn_block_number, txn_index;
