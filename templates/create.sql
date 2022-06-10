with verified_contracts as (
    select distinct contract_address, abi
    from ethereum.deployed_contract_metadata
    {{ $length := len .ContractList }} {{ if ne $length 0 }}
        where contract_address = '0x00'
            {{ range $contractAddress := .ContractList }}
            or contract_address = '{{ $contractAddress }}'
            {{ end }}
    {{ end }}
)

{{ $length := len .ContractList }} {{ if ne $length 0 }}
    select * from verified_contracts;
    {{ else }}
        select
            l.address as contract_address,
            c.abi as abi
        from ethereum.logs l
        join verified_contracts c on l.address = c.contract_address
        group by 1, 2
        having count(*) >= {{ .Count }}
        {{ if .AddLimit }}
        limit {{ .Limit }}
        {{ end }}
        ;
{{ end }}
