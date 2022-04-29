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
(CONNECT_DATA =      (SERVER = DEDICATED)      (SERVICE_NAME = TESEO)    )  )"""
              
def conectar(entorno):
    global connection
    global cursor

    try:
        connection = cx_Oracle.connect(user=username, password=userpwd, dsn=dsn, encoding=encoding)
        cursor = connection.cursor()

    except cx_Oracle.Error as error:
        print("Error al conectar")
        print(error)
       
def buscar_registro(codigo):
    sql = """select wh.work_header_id, wh.cod_sgae, wh.work_type_id, dc1.doc_date, wc.start_effectivity_date, 
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
    where  wh.cod_sgae = :cod """

    cursor.execute(sql, [codigo])
    
    temp = cursor.fetchone()
    
    if temp:
      return temp
      
    return None

def guardar(row):
    with open('resultados.csv','a', newline='') as file:
        registros = csv.writer(file, delimiter = ',')
        registros.write(row)

def main ():
    conectar(1)

    with open('archivo.csv') as file:
        reader = csv.DictReader(file)

        for row in reader:
            codigo = row['SGAECODE']
            print(codigo)

            tmp = buscar_registro(codigo)

            if tmp:
                guardar(tmp)


main()                

  
