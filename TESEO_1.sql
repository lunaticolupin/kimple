-- Construcción de Ficha y Ponderación --
--declare
--begin
    select wh.work_header_id, wh.cod_sgae, wh.work_type_id, dc1.doc_date, wc.start_effectivity_date, 
    dc.percentage_d, dc.percentage_f, dc.percentage_l, dc.percentage_m,
    cmp.percentage, cmp.length, cs1.composition_length, cs1.percentage,
    w.distribution_typology_id, w.nationality_id, w.work_length w_work_length,
    w.music_length w_music_length, e.work_length w_work_length, e.music_length e_music_length, 
    v.version_id,
    case when dc.contract_header_id is null and dc1.contract_header_id is null 
    then 1
    else 0 
    end contrato_default
    from t_aud.work_header wh 
    left join t_aud.composition_cmp cmp on cmp.work_header_id = wh.work_header_id
    left join t_aud.cue_sheet cs on cs.work_header_id = wh.work_header_id
    left join T_AUD.composition_cue_sheet cs1 on cs1.work_header_id = wh.work_header_id
    join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id 
    join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id 
    left join t_aud.distribution_contract dc on dc.contract_header_id = ch.contract_header_id
    left join t_aud.declaration_contract dc1 on dc1.contract_header_id = ch.contract_header_id
    left join t_aud.work w on w.work_header_id = wh.work_header_id
    left join t_aud.episode e on e.work_header_id = wh.work_header_id
    left join t_aud.version v on v.work_header_id = wh.work_header_id
    where  wc.start_effectivity_date between to_date('2021-01-01','yyyy-mm-dd') 
    and to_date('2022-01-02','yyyy-mm-dd') and dc.distribution_key_id = 2
    and rownum <= 1000 
    order by contrato_default desc, wc.start_effectivity_date desc;
-- end;