
select * 
from t_aud.work_header wh 
join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id 
join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id 
left join t_aud.distribution_contract dc on dc.contract_header_id = ch.contract_header_id
left join t_aud.declaration_contract dc1 on dc1.contract_header_id = ch.contract_header_id
left join t_aud.work w on w.work_header_id = wh.work_header_id
where wh.cod_sgae= '50137347' and wc.start_effectivity_date
< to_date('2021-03-01','yyyy-mm-dd');