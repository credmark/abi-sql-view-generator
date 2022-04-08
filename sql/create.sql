with verified_contracts as (
    select distinct contract_address, contract_name, abi
    from deployed_contract_metadata
)

select
    l.address as contract_address,
    c.contract_name as contract_name,
    c.abi as abi,
    count(*) as n_logs
from logs l
join verified_contracts c on l.address = c.contract_address
group by 1, 2, 3
order by 4 desc
limit 10;