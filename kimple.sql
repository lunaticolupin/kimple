-- select count(*) from etl_wt01 where sgaecode is not null;

create table ficha_av (
work_header_id number,
cod_sgae varchar2(100 char),
work_type_id number,
doc_date date,
start_effectivity_date date,
percentage_d number,
percentage_f number,
percentage_l number,
percentage_m number,
cmp_percentage number,
cmp_length number,
css_lenght number,
css_percentage number,
distribution_typology_id number,
nationality_id number,
w_work_length number,
w_music_length number,
e_work_length number,
e_music_length number,
version_id number,
contrato_default number,
primary key(cod_sgae)
);


select * from etl_wt01 where sgaecode is not null;
