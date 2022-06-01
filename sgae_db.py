import cx_Oracle
import csv 

username = 'ETLGLOBAL'
userpwd = 'HqJc0nl4pA'
encoding = 'UTF-8'
cursor = None
connection = None

dsn = """(DESCRIPTION =   
(ADDRESS = (PROTOCOL = TCP)(HOST = 10.133.3.57)(PORT = 1521))   
(ADDRESS = (PROTOCOL = TCP)(HOST = 10.133.3.58)(PORT = 1521))    
(LOAD_BALANCE = yes)    
(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = TESEO)))"""


def conectar(entorno):
    global connection
    global cursor
    result = False

    try:
        connection = cx_Oracle.connect(user=username, password=userpwd, dsn=dsn, encoding=encoding)
        cursor = connection.cursor()
        result = True
    except cx_Oracle.Error as error:
        print("Error al conectar")
        print(error)

    return result


def filiacion(codigo, distribucion_part):
    roles = ""
    if distribucion_part == 1:
        roles = "'RE','CS','CE'"

    elif distribucion_part == 2:
        roles = "'AS','DS','CS','CE'"

    elif distribucion_part == 5:
        roles = "'DS','AS','CS','CE'"

    sql = """SELECT
    *
FROM
    (
        SELECT DISTINCT
            t1.ip2_ipn_id AS ip_name_id,
            t3.ip2_ipb_id AS ip_base,
            t2.ip2_ipb_member_code AS ip_member_code,
            t2.ip2_ipb_code AS ip_base_code,
            t1.ip2_ipn_name
            || ' '
            || t1.ip2_ipn_first_name AS indexed_name,
            (
                CASE
                    WHEN t1.ip2_nsr_id = 2
                         AND t1.ip2_ipn_internal = 1 THEN 1
                    ELSE 0
                END
            ) AS internal,
            t2.ip2_ipb_int_role AS role,
            nvl(lpad(t41.ip2_mbr_soc_code,3,'0'),'099') AS soc_code,
            t41.ip2_mbr_cc_code AS cc_code,
            t41.ip2_mbr_rl_code,
            t41.ip2_mbr_rg_code AS rg_code,
            t41.ip2_mbr_dt_from,
            t41.ip2_mbr_dt_to,
            t41.ip2_mbr_ter_id,
            t1.ip2_nty_id
        FROM
            t_ip2_ip_base_memberships t41,
            t_ip2_ip_names t1,
            t_ip2_ip_bases t2,
            t_ip2_ip_base_names t3
        WHERE
            t3.ip2_ipn_id (+) = t1.ip2_ipn_id
            AND t2.ip2_ipb_id (+) = t3.ip2_ipb_id
            AND t41.ip2_mbr_ip_base (+) = t2.ip2_ipb_id
            AND t3.ip2_ipb_id IS NOT NULL
        UNION
        SELECT DISTINCT
            t1.ip2_ipn_id AS ip_name_id,
            t3.ip2_ipb_id AS ip_base,
            t2.ip2_ipb_member_code AS ip_member_code,
            t2.ip2_ipb_code AS ip_base_code,
            t1.ip2_ipn_name
            || ' '
            || t1.ip2_ipn_first_name AS indexed_name,
            (
                CASE
                    WHEN t1.ip2_nsr_id = 2
                         AND t1.ip2_ipn_internal = 1 THEN 1
                    ELSE 0
                END
            ) AS internal,
            t2.ip2_ipb_int_role AS role,
            nvl(lpad(t41.ip2_mbr_soc_code,3,'0'),'099') AS soc_code,
            t41.ip2_mbr_cc_code AS cc_code,
            t41.ip2_mbr_rl_code,
            t41.ip2_mbr_rg_code AS rg_code,
            t41.ip2_mbr_dt_from,
            t41.ip2_mbr_dt_to,
            t41.ip2_mbr_ter_id,
            t1.ip2_nty_id
        FROM
            t_ip2_ip_base_memberships t41,
            t_ip2_ip_names t1,
            t_ip2_ip_bases t2,
            t_ip2_ip_base_names t3,
            t_ip2_ip_ot_names t4
        WHERE
            t4.ip2_otn_ot_ip_name = t1.ip2_ipn_id
            AND t3.ip2_ipn_id (+) = t4.ip2_otn_ip_name
            AND t2.ip2_ipb_id (+) = t3.ip2_ipb_id
            AND t41.ip2_mbr_ip_base (+) = t2.ip2_ipb_id
    ) t_ipi
WHERE
    ip_name_id =:codigo
    AND (
        ip2_mbr_dt_from IS NULL
        OR TO_DATE(ip2_mbr_dt_from,'yyyy-MM-dd hh24:mi:ss') <= TO_DATE(:fecha,'yyyy-MM-dd hh24:mi:ss')
    )
    AND (
        ip2_mbr_dt_to IS NULL
        OR TO_DATE(ip2_mbr_dt_to,'yyyy-MM-dd hh24:mi:ss') >= TO_DATE(:fecha,'yyyy-MM-dd hh24:mi:ss')
    )
    AND cc_code IN (:listaClaseCreacion)
    AND ip2_mbr_rl_code IN (:listaRoles)
    AND rg_code IN (:listaTipoDerecho)
ORDER BY
    DECODE(cc_code,'AV','CC_CODE1','AF','CC_CODE2','AD','CC_CODE3','DW','CC_CODE4','LW','CC_CODE5')"""
    values = (codigo, '2021-08-18', '2021-08-18', 'AV', roles, "'TB','PR','RT'")

    cursor.execute(sql, values)

    temp = cursor.fetchone()

    if temp:
        return temp[7]

    return None


def proteccion_sgae(soc_code):
    if soc_code in ('072', '099'):
        return True

    sql = "select * from NON_ADMIN_CATALOG where society_code = :codigo"
    cursor.execute(sql, [soc_code])

    temp = cursor.fetch()

    if temp:
        if temp[7] == 1:
            return True
        else:
            return False

    return False


def buscar_registro(codigo):
    ''' sql = """select wh.work_header_id, wh.cod_sgae, wh.work_type_id, dc1.doc_date, wc.start_effectivity_date,
    dc1.PERCENTAGE, dc.percentage_d, dc.percentage_f, dc.percentage_l, dc.percentage_m,
    cmp.percentage, cmp.length, cs1.composition_length, cs1.percentage,
    w.distribution_typology_id, w.nationality_id, w.work_length w_work_length,
    w.music_length w_music_length, e.work_length w_work_length, e.music_length e_music_length, 
    v.version_id, cs.MOD_DATE fecha_devengo,
    case when dc.contract_header_id is null and dc1.contract_header_id is null 
    then 1
    else 0 
    end contrato_default
    from t_aud.work_header wh 
    left join t_aud.composition_cmp cmp on cmp.work_header_id = wh.work_header_id
    left join t_aud.cue_sheet cs on cs.work_header_id = wh.work_header_id
    left join T_AUD.composition_cue_sheet cs1 on cs1.work_header_id = wh.work_header_id
    left join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id 
    left join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id 
    left join t_aud.distribution_contract dc on dc.contract_header_id = ch.contract_header_id
    left join t_aud.declaration_contract dc1 on dc1.contract_header_id = ch.contract_header_id
    left join t_aud.work w on w.work_header_id = wh.work_header_id
    left join t_aud.episode e on e.work_header_id = wh.work_header_id
    left join t_aud.version v on v.work_header_id = wh.work_header_id
    where  wh.cod_sgae = :cod """'''

    # sql = """SELECT DISTINCT wh.work_header_id, wh.cod_sgae, wh.work_type_id, dc.CONTRACT_HEADER_ID,
    # dc.DECLARATION_CONTRACT_ID,  dc.PROFESSION_ID, dc.COD_IPI,  dc.PERCENTAGE, dc2.percentage_f, dc2.percentage_l,
    # dc2.percentage_m, cmp.percentage cmp_percentage, cmp.LENGTH cmp_LENGTH , cs.MOD_DATE fecha_devengo,
    # cs1.composition_length cs_composition_length, cs1.percentage cs_percentage,  w.distribution_typology_id,
    # w.nationality_id, w.work_length w_work_length, w.music_length w_music_length, e.work_length w_work_length,
    # e.music_length e_music_length, v.version_id
    # FROM T_AUD.WORK_HEADER wh
    # JOIN T_AUD.WORK_CONTRACT wc ON wc.WORK_HEADER_ID = wh.WORK_HEADER_ID
    # JOIN T_AUD.CONTRACT_HEADER ch ON ch.CONTRACT_HEADER_ID = wc.CONTRACT_HEADER_ID -- AND ch.CONTRACT_TYPE_ID = 1
    # LEFT JOIN T_AUD.DECLARATION_CONTRACT dc ON dc.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID
    # LEFT JOIN T_AUD.DISTRIBUTION_CONTRACT dc2 ON dc2.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID
    # LEFT JOIN t_aud.composition_cmp cmp on cmp.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID IN (1,2)
    # LEFT join t_aud.cue_sheet cs on cs.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID IN (1,2)
    # LEFT join T_AUD.composition_cue_sheet cs1 on cs1.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID IN (1,2)
    # left join T_AUD.WORK w  on w.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID = 1
    # left join t_aud.episode e on e.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID = 5
    # left join t_aud.version v on v.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID IN (4,5)
    # WHERE wh.COD_SGAE = :COD_SGAE"""

    # sql = """SELECT COD_SGAE, wh.WORK_TYPE_ID,  wc.START_EFFECTIVITY_DATE,  ch.contract_type_id, dc.COD_IPI, count(dc.DECLARATION_CONTRACT_ID) num_contratos, dc.DISTRIBUTION_PART_ID,
    # sum(nvl(dc.PERCENTAGE, 50)) porcentaje_contrato, dc2.DISTRIBUTION_KEY_ID, nvl(dc2.PERCENTAGE_D, 25) percentage_d, dc2.PERCENTAGE_f, nvl(dc2.PERCENTAGE_L, 50) percentage_l,
    # nvl(dc2.PERCENTAGE_M, 25) percentaje_m
    # from t_aud.work_header wh
    # join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id
    # join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id
    # LEFT JOIN T_AUD.DECLARATION_CONTRACT dc ON dc.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID
    # LEFT JOIN T_AUD.DISTRIBUTION_CONTRACT dc2 ON dc2.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID
    # LEFT join t_aud.cue_sheet cs on cs.work_header_id = wh.work_header_id AND wh.WORK_TYPE_ID IN (1,2)
    # where wh.cod_sgae= :COD_SGAE
    # GROUP BY COD_SGAE, wh.WORK_TYPE_ID, wc.START_EFFECTIVITY_DATE, ch.CONTRACT_TYPE_ID, dc.COD_IPI, dc.DISTRIBUTION_PART_ID, dc2.DISTRIBUTION_KEY_ID , dc2.PERCENTAGE_D ,
    # dc2.PERCENTAGE_D, dc2.PERCENTAGE_F, dc2.PERCENTAGE_L, dc2.PERCENTAGE_M"""
    sql = """SELECT wh.COD_SGAE, wh.WORK_HEADER_ID, dc.CONTRACT_HEADER_ID, wh.WORK_TYPE_ID, 
    nvl(w2.DISTRIBUTION_TYPOLOGY_ID, w.DISTRIBUTION_TYPOLOGY_ID) tipologia,  
    nvl(w2.NATIONALITY_ID, w.NATIONALITY_ID) nacionalidad, wc.START_EFFECTIVITY_DATE,  ch.contract_type_id, 
    dc.COD_IPI, dc.DECLARATION_CONTRACT_ID, dc.PROFESSION_ID, dc2.DISTRIBUTION_KEY_ID, 
    dc.PERCENTAGE porcentaje_contrato, nvl(dc2.PERCENTAGE_D, 25) porcentaje_d, 
    nvl(dc2.PERCENTAGE_L, 50) porcentaje_l, nvl(dc2.PERCENTAGE_M, 25) porcentaje_m
    from t_aud.work_header wh 
    join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id 
    join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id 
    LEFT JOIN T_AUD.DECLARATION_CONTRACT dc ON dc.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID 
    LEFT JOIN T_AUD.DISTRIBUTION_CONTRACT dc2 ON dc2.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID 
    LEFT JOIN T_AUD.EPISODE e ON e.WORK_HEADER_ID = wh.WORK_HEADER_ID AND wh.WORK_TYPE_ID = 3 
    AND e.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd')
    LEFT JOIN T_AUD.WORK_HEADER wh3 ON wh3.WORK_HEADER_ID = e.SERIES_HEADER_ID AND wh.WORK_TYPE_ID = 3
    LEFT JOIN T_AUD."WORK" w ON w.WORK_HEADER_ID = wh3.WORK_HEADER_ID AND wh.WORK_TYPE_ID = 3
    LEFT JOIN T_AUD."WORK" w2 ON w2.WORK_HEADER_ID = wh.WORK_HEADER_ID AND wh.WORK_TYPE_ID = 1
    where wh.cod_sgae= :COD_SGAE and wc.START_EFFECTIVITY_DATE = (
    SELECT max(wc.START_EFFECTIVITY_DATE) FROM T_AUD.WORK_HEADER wh2
    JOIN T_AUD.WORK_CONTRACT wc ON wc.WORK_HEADER_ID = wh2.WORK_HEADER_ID 
    WHERE wh2.cod_sgae = wh.COD_SGAE  and wc.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd'))"""

    cursor.execute(sql, [codigo])
    
    temp = cursor.fetchall()
    
    if temp:
        return temp
      
    return []


def guardar(row):
    with open('resultados_12.csv', 'a', newline='') as file:
        registros = csv.writer(file, delimiter=',')
        registros.writerow(row)


def main():
    if not conectar(1):
        return

    encontrados = 0

    with open('ETL_BROADCASTS_2.csv') as file:
        reader = csv.DictReader(file)

        headers = ["COD_SGAE", "WORK_HEADER_ID", "CONTRACT_HEADER_ID", "WORK_TYPE_ID", "TIPOLOGIA", "NACIONALIDAD",
                   "FECHA_INCIO", "CONTRACT_TYPE", "COD_IPI", "DECLARATION_CONTRACT_ID", "PROFESION_ID",
                   "DISTRIBUTION_KEY_ID", "PORCENTAJE_CONTRATO", "PORCENTAJE_D", "PORCENTAJE_L", "PORCENTAJE_M"]

        guardar(headers)

        for row in reader:
            codigo = row['SGAECODE']
            print(f'{reader.line_num} - {codigo}')

            tmp = buscar_registro(codigo)

            for t in tmp:
                guardar(t)

            if len(tmp):
                encontrados += 1

    print(f'{encontrados} de {reader.line_num}')


main()
