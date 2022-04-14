CREATE OR REPLACE VIEW ethereum_contracts.{{ .Namespace }}_{{ .ContractAddress }}_evt_{{ .Name }}
    AS
        SELECT
            '{{ .ContractAddress }}' as contract_address
            ,log_index as evt_index
            ,block_number as evt_block_number
            ,transaction_hash as evt_tx_hash
            {{ range .Inputs }}
            ,ethereum_contracts.decode_abi_input_parameter_prod(substring({{ .ColumnName }}, {{ .StartPos }}, {{ .Length }}), '{{ .InputType }}') AS inp_{{ .InputName }}
            {{ end }}
        FROM ethereum.logs
        WHERE address = '{{ .ContractAddress }}' AND substring(topics, 1, 66) = '{{ .SigHash }}'
        ORDER BY evt_block_number, evt_index;
