with verified_contracts as (
    select distinct contract_address, contract_name, abi
    from ethereum.deployed_contract_metadata
)

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
