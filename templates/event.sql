CREATE OR REPLACE VIEW {{ .Namespace }}_{{ .ContractAddress }}_evt_{{ .Name }}
    AS
        SELECT
            '{{ .ContractAddress }}' as contract_address
            ,log_index as evt_index
            ,block_number as evt_block_number
            ,transaction_hash as evt_tx_hash
            {{ range .Inputs }}
            ,decode_abi_input_parameter_dev(substring({{ .ColumnName }}, {{ .StartPos }}, {{ .Length }}), '{{ .InputType }}') AS inp_{{ .InputName }}
            {{ end }}
        FROM logs
        WHERE address = '{{ .ContractAddress }}' AND substring(topics, 1, 66) = '{{ .SigHash }}'
        ORDER BY evt_block_number, evt_index;
