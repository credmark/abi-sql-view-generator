CREATE OR REPLACE VIEW ethereum_contracts.{{ .Namespace }}_{{ .ContractAddress }}_fn_{{ .Name }}
    AS
        WITH UNIONED AS (
            SELECT
                '{{ .ContractAddress }}' as contract_address
                ,hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                {{ range .Inputs }}
                ,ethereum_contracts.decode_abi_input_parameter_prod(substring({{ .ColumnName }}, {{ .StartPos }}, {{ .Length }}), '{{ .InputType }}') AS inp_{{ .InputName }}
                {{ end }}
            FROM ethereum.transactions
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'

            UNION

            SELECT 
                '{{ .ContractAddress }}' as contract_address
                ,transaction_hash as txn_hash
                ,block_number as txn_block_number
                ,transaction_index as txn_index
                {{ range .Inputs }}
                ,ethereum_contracts.decode_abi_input_parameter_prod(substring({{ .ColumnName }}, {{ .StartPos }}, {{ .Length }}), '{{ .InputType }}') AS inp_{{ .InputName }}
                {{ end }}
            FROM ethereum.traces
            WHERE to_address='{{ .ContractAddress }}' AND substring(input, 1, 10)='{{ .MethodIdHash }}'
        )

        SELECT *
        FROM UNIONED
        ORDER BY txn_block_number, txn_index;
