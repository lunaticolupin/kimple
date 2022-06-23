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


def filiacion(codigo_ipi, distribucion_part):
    roles = ""
    if distribucion_part == 1:
        roles = "'RE','CS','CE'"

    elif distribucion_part == 2:
        roles = "'AS','DS','CS','CE'"

    elif distribucion_part == 5:
        roles = "'DS','AS','CS','CE'"

    sql = f"""SELECT
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
    AND ip2_mbr_rl_code IN ({roles})
    AND rg_code IN ('TB','PR','RT')
ORDER BY
    DECODE(cc_code,'AV','CC_CODE1','AF','CC_CODE2','AD','CC_CODE3','DW','CC_CODE4','LW','CC_CODE5')"""
    values = (codigo_ipi, '2021-08-19 00:00:00', '2021-08-19 00:00:00', 'AV')

    cursor.execute(sql, values)
    #print(cursor.statement)

    temp = cursor.fetchone()

    if temp:
        return temp[7]

    return None


def proteccion_sgae(soc_code):
    if soc_code in ('072', '099'):
        return True

    sql = "select * from NON_ADMIN_CATALOG where SOCIETY_COD = :codigo"
    cursor.execute(sql, [soc_code])

    temp = cursor.fetchone()

    if temp:
        if temp[7] == 1:
            return True
        else:
            return False

    return False


def buscar_registro(codigo):
    sql = """SELECT t.*, 
    CASE WHEN distribution_part_id = 1 THEN (percentage*porcentaje_d)/(porcentaje_d+porcentaje_l+nvl(porcentaje_f,0))
    WHEN distribution_part_id = 2 THEN (percentage*porcentaje_l)/(porcentaje_d+porcentaje_l+nvl(porcentaje_f,0))
    WHEN distribution_part_id = 5 THEN (percentage*porcentaje_f)/(porcentaje_d+porcentaje_l)
    ELSE NULL END proteccion_sgae
    FROM (SELECT DISTINCT  wh.COD_SGAE, wh.WORK_HEADER_ID, ch.contract_header_id,  wh.WORK_TYPE_ID, dc3.VERSION, w.DISTRIBUTION_TYPOLOGY_ID, 
    w.NATIONALITY_ID, wc.START_EFFECTIVITY_DATE,  ch.contract_type_id, dc3.
    dc.COD_IPI, dc.DECLARATION_CONTRACT_ID, dc.PROFESSION_ID, nvl(dc2.DISTRIBUTION_KEY_ID, dc3.DISTRIBUTION_KEY_ID) distribution_key_id,  dc.DISTRIBUTION_PART_ID, 
    nvl(dc.PERCENTAGE, 100)PERCENTAGE,  nvl(dc2.PERCENTAGE_D, dc3.PERCENTAGE_D) porcentaje_d, nvl(dc2.PERCENTAGE_L, dc3.PERCENTAGE_L) porcentaje_l, nvl(dc2.PERCENTAGE_M, dc3.PERCENTAGE_M) porcentaje_m,
    nvl(dc2.PERCENTAGE_F, dc3.PERCENTAGE_F) porcentaje_f
    from t_aud.work_header wh 
    join t_aud.work_contract wc on wc.work_header_id = wh.work_header_id 
    join t_aud.contract_header ch on ch.contract_header_id = wc.contract_header_id 
    LEFT JOIN T_AUD.DECLARATION_CONTRACT dc ON dc.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID AND 
    dc.START_EFFECTIVITY_DATE <  to_date('2021-08-18','yyyy-mm-dd') 
    AND (dc.END_EFFECTIVITY_DATE IS NULL OR dc.END_EFFECTIVITY_DATE > to_date('2021-08-18','yyyy-mm-dd'))
    LEFT JOIN T_AUD.DISTRIBUTION_CONTRACT dc2 ON dc2.CONTRACT_HEADER_ID = ch.CONTRACT_HEADER_ID 
    AND dc2.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd') 
    AND (dc2.END_EFFECTIVITY_DATE IS NULL OR dc2.END_EFFECTIVITY_DATE > to_date('2021-08-18','yyyy-mm-dd')) 
    AND dc2.DISTRIBUTION_KEY_ID = 2
    LEFT JOIN T_AUD."WORK" w ON w.WORK_HEADER_ID = wh.WORK_HEADER_ID 
    OR (w.WORK_HEADER_ID in (SELECT e.SERIES_HEADER_ID FROM T_AUD.EPISODE e WHERE e.WORK_HEADER_ID=wh.WORK_HEADER_ID) 
    AND wh.WORK_TYPE_ID IN (2,3))
    OR (w.WORK_HEADER_ID IN (SELECT v.ORIGINAL_WORK_HEADER_ID  FROM T_AUD.VERSION v 
    WHERE v.WORK_HEADER_ID=wh.WORK_HEADER_ID) AND wh.WORK_TYPE_ID = 4)
    OR (w.WORK_HEADER_ID IN (SELECT v3.ORIGINAL_WORK_HEADER_ID  FROM T_AUD.VERSION v2, T_AUD.VERSION v3 
    WHERE v2.WORK_HEADER_ID=wh.WORK_HEADER_ID 
    AND v2.SERIES_HEADER_ID=v3.WORK_HEADER_ID) AND wh.WORK_TYPE_ID IN (5,6))
    AND w.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd') AND (w.END_EFFECTIVITY_DATE IS NULL OR 
    to_date('2021-08-18','yyyy-mm-dd') < w.END_EFFECTIVITY_DATE)
    LEFT JOIN T_AUD.DEFAULT_CONTRACT dc3 ON dc3.VERSION = CASE WHEN wh.WORK_TYPE_ID IN (1,2,3) THEN 'O' ELSE 'V' END 
    AND dc3.DISTRIBUTION_TYPOLOGY_ID = w.DISTRIBUTION_TYPOLOGY_ID 
    AND dc3.NATIONALITY_ID IN (-1,w.NATIONALITY_ID) AND dc3.START_DISTRIBUTION_DATE < to_date('2021-08-18','yyyy-mm-dd') 
    and dc3.DISTRIBUTION_KEY_ID = 2 
    and dc3.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd') 
    and (dc3.END_EFFECTIVITY_DATE is null or to_date('2021-08-18','yyyy-mm-dd') < dc3.END_EFFECTIVITY_DATE)
    and dc3.START_DISTRIBUTION_DATE  < to_date('2021-08-18','yyyy-mm-dd')  
    AND (dc3.END_DISTRIBUTION_DATE IS NULL OR to_date('2021-08-18','yyyy-mm-dd') < dc3.END_DISTRIBUTION_DATE) 
    AND dc3.ASSOCIATION_ID = 1
    where wh.cod_sgae= :COD_SGAE and wc.START_EFFECTIVITY_DATE < to_date('2021-08-18','yyyy-mm-dd')) t"""

    cursor.execute(sql, [codigo])
    
    temp = cursor.fetchall()
    
    if temp:
        return temp
      
    return []


def guardar(row):
    with open('resultados_14.csv', 'a', newline='') as file:
        registros = csv.writer(file, delimiter=',')
        registros.writerow(row)


def main():
    if not conectar(1):
        return

    encontrados = 0

    with open('ETL_BROADCASTS_2.csv') as file:
        reader = csv.DictReader(file)

        headers = ["COD_SGAE", "WORK_HEADER_ID", "CONTRACT_HEADER_ID", "WORK_TYPE_ID", "VERSION", "TIPOLOGIA",
                   "NACIONALIDAD", "FECHA_INCIO", "CONTRACT_TYPE", "COD_IPI", "DECLARATION_CONTRACT_ID", "PROFESION_ID",
                   "DISTRIBUTION_KEY_ID", "DISTRIBUTION_PART_ID", "PORCENTAJE_CONTRATO", "PORCENTAJE_D", "PORCENTAJE_L",
                   "PORCENTAJE_M", "PORCENTAJE_PROTECCION"]

        guardar(headers)

        for row in reader:
            codigo = row['SGAECODE']
            print(f'{reader.line_num} - {codigo}')

            tmp = buscar_registro(codigo)

            for t in tmp:
                if t[9] is None or t[13] is None:
                    print("No se puede determinar la filiacion")
                    continue

                cod_filiacion = filiacion(t[9], t[13])

                if proteccion_sgae(cod_filiacion):
                    encontrados += 1
                    guardar(t)
                else:
                    print(f"{t[0]} - {t[9]} - {t[13]} no afiliado a SGAE")

            # if len(tmp):

    print(f'{encontrados} de {reader.line_num}')


main()
