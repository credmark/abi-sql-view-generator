CREATE OR REPLACE VIEW ethereum_contracts.{{ .Namespace }}_{{ .ContractAddress }}_evt_{{ .Name }}
    AS
        WITH q as (
            SELECT
                '{{ .ContractAddress }}' as contract_address
                ,log_index as evt_index
                ,block_number as evt_block_number
                ,transaction_hash as evt_tx_hash
                ,ethereum_contracts.decode_abi_input_prod(data, topics, parse_json('{{ .InputsJson }}'), 'event', true) as val
            FROM ethereum.logs
            WHERE address = '{{ .ContractAddress }}' AND substring(topics, 1, 66) = '{{ .SigHash }}'
        )
        SELECT
            contract_address
            ,evt_block_number
            ,evt_tx_hash
            ,evt_index
            {{ range .Inputs }}
            ,val:{{ .Name }} as inp_{{ .Name }}
            {{ end }}
        FROM q
        ORDER BY evt_block_number, evt_index;
