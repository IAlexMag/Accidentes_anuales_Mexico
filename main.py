#%%
import pandas as pd
import os
import pymysql.cursors
from dotenv import load_dotenv
from pymysql import Error

load_dotenv()

connection = pymysql.connect(host= os.getenv('HOST'),
                             port=int(os.getenv('PORT')),
                             user=os.getenv('USER'),
                             password=os.getenv('PASSWORD'),
                             database=os.getenv('DATABASE'))

with connection.cursor() as cursor:
     cursor.execute("SELECT DISTINCT(anio) FROM tr_cifra;")
     result = cursor.fetchall()
     cursor.close()
print(result)
#%%
folder = "C:/Users/alexa/Downloads/accidentes_anuales_del_pais/conjunto_de_datos"

regs = 0
try:
    for file in os.listdir(folder):
        if file.endswith('.csv'):
            root = os.path.join(folder, file)
            df = pd.read_csv(root, index_col=False)
            df.fillna({'CAUSAACCI' : 'Sin causa registrada'}, inplace=True)
            df.fillna({'CAPAROD' : 'Sin capa registrada'}, inplace=True)
            df.fillna({'SEXO' : 'Sin dato registrado'}, inplace=True)
            df.fillna({'ALIENTO' : 'Sin dato registrado'}, inplace=True)
            df.fillna({'CINTURON' : 'Sin dato registrado'}, inplace=True)
            df.fillna({'CLASACC' : 'Sin dato registrado'}, inplace=True)
            df.fillna({'ESTATUS' : 'Sin dato registrado'}, inplace=True)
            df.fillna({'COBERTURA' : 'Sin dato registrado'}, inplace=True)
            sql = "CALL insert_data (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,@msg)"
            try:   
                with connection.cursor() as cursor:     
                    for index, row in df.iterrows():
                        values = tuple(row)
                        cursor.execute(sql, values)             #print(f'{sql}, {values}')
                        regs += 1
                        print(regs)
                connection.commit()
            except Error as e:
                    connection.rollback()
                    print(f'El error es: {e}')
except ValueError as e:
    print(e)
finally:
     connection.close()


'''INSERT INTO tr_cifra (cobertura,id_entidad,id_municipio,anio,id_mes,id_hora,id_minuto,id_dia,diasemana,urbana,suburbana,tipacc,automovil,
                        campasaj,microbus,pascamion,omnibus,tranvia,camioneta,camion,tractor,ferrocarril,motocicleta,bicicleta,otro_vehic,causaacc,caparod,sexo,aliento,
                        cinturon,id_edad,condmuerto,condherido,pasamuerto,pasaherido,peatmuerto,peatherido,ciclmuerto,ciclherido,otromuerto,otroherido,nemuerto,neherido,clasacc,estatus)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        
                        
                        values = (row['COBERTURA'], row['ID_ENTIDAD'], row['ID_MUNICIPIO'], row['ANIO'], row['MES'], row['ID_HORA'], row['ID_MINUTO'], row['ID_DIA'],
                                row['DIASEMANA'], row['URBANA'], row['SUBURBANA'], row['TIPACCID'], row['AUTOMOVIL'], row['CAMPASAJ'], row['MICROBUS'], row['PASCAMION'],
                                row['OMNIBUS'], row['TRANVIA'], row['CAMIONETA'], row['CAMION'], row['TRACTOR'], row['FERROCARRI'], row['MOTOCICLET'], row['BICICLETA'],
                                row['OTROVEHIC'], row['CAUSAACCI'], row['CAPAROD'], row['SEXO'], row['ALIENTO'], row['CINTURON'], row['ID_EDAD'], row['CONDMUERTO'], row['CONDHERIDO'],
                                row['PASAMUERTO'], row['PASAHERIDO'], row['PEATMUERTO'], row['PEATHERIDO'], row['CICLMUERTO'], row['CICLHERIDO'], row['OTROMUERTO'], row['OTROHERIDO'],
                                row['NEMUERTO'], row['NEHERIDO'], row['CLASACC'], row['ESTATUS'])'''