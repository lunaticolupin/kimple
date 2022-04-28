import cx_Oracle

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
    sql = ""
    cursor.execute(sql, {'_cod_': codigo})
    
    temp = cursor.fetchone()
    
    if temp:
      return temp
      
    return None
  
