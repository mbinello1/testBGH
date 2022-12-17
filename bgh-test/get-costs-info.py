#FALTA
#ver lo del api call al cost explorer
#REVISAR si asi esta bien el tema de lambda, como hago para importar pandas a lambda en aws??
#REVISAR el tema del directorio, donde se guardaria o como hacemos? 
    #me imagino que en vez de llamar al csv, si hago a sentencia del api call y que use el archivo de una
    #va a funcionar mejor

#LISTO
#Llamar al csv del resultado
#La logica del codigo esta armada
#la subida a s3 en teoria deberia funcionar, sino aparte esta guardandose local
#usar lambda para ejecutar el codigo

import pandas as pd
import os
import boto3

#vamos al directorio donde tenemos guardado el archivo de la api del cost explorer
os.chdir('c:/Users/Matias/Documents/GitHub/testBGH/bgh-test')

#primero queremos traer la info de la api del cost explorer
#damos por hecho que la info del cost explorer es del csv que pasaron?
#o hacemos un api call simulando el pedidode info y que el resultado es el csv?

#traemos la info del csv
def get_info():
    data = pd.read_csv('cost_info.csv')
    return data

#transformamos la data para que este en formato numerico
def transform_data(df):
    #generamos un nuevo df que va a contener la informacion limpia
    df_clean = pd.DataFrame(columns = ['Cliente', 'consumo_mensual'])
    for i in range(len(df)):
        #esta primera linea guarda la inforamcion del cliente
        df_clean = df_clean.append({'Cliente' : df.loc[i]['Cliente'], 
        #en esta segunda parte, primero buscamos la posicion del signo $ para movernos un caracter
        #a la derecha y asi obtener todo el numero
        #despues usamos replace() para eliminar el "." y poder convertir el texto a float
        'consumo_mensual' : float(df.loc[i]['Consumo mensual AWS'][df.loc[i]['Consumo mensual AWS'].find('$') + 1:].replace(".",""))},
        ignore_index = True)
        #print(df_clean.loc[i])
        #print()
    return df_clean

#se genera un nuevo df las 2 columnas a insertar/calcular
def info(df):
    df_2 = df
    df_2['facturacion_local'] = 0.0
    df_2['cobro_total'] = 0.0
    for i in range(len(df_2)):
        #print(df_2.loc[i])
        #print()
        #los if de abajo se fijan el consumo y hacen los calculos necesarios segun cada escenario
        if df_2.loc[i]['consumo_mensual'] < 1000:
            df_2.loc[i,'facturacion_local'] = 250.0
            df_2.loc[i,'cobro_total'] = df_2.loc[i,'consumo_mensual'] + df_2.loc[i,'facturacion_local']
        elif df_2.loc[i]['consumo_mensual'] >= 1000 and df_2.loc[i]['consumo_mensual'] < 5000:
            df_2.loc[i,'facturacion_local'] = df_2.loc[i,'consumo_mensual'] * 0.30
            df_2.loc[i,'cobro_total'] = df_2.loc[i,'consumo_mensual'] + df_2.loc[i,'facturacion_local']
        elif df_2.loc[i]['consumo_mensual'] >= 5000:
            df_2.loc[i,'facturacion_local'] = df_2.loc[i,'consumo_mensual'] * 0.35
            df_2.loc[i,'cobro_total'] = df_2.loc[i,'consumo_mensual'] + df_2.loc[i,'facturacion_local']
        else:
            print('Error')
    return df_2

#con eso listo, lo guardamos en un csv que se sube a un bucket de S3
def upload_file_to_s3(file):
    s3 = boto3.resource("s3")
    bucket_name = "bucket-a-usar" #aca va el bucket donde queremos guardar la info
    file_name = file
    object_name = "aws_monthly_costs"
    bucket = s3.Bucket(bucket_name)
    response = bucket.upload_file(file_name, object_name)

def lambda_handler(event, context):
    file = info(transform_data(get_info())).to_csv('costs.csv')
    #se guarda el archivo en el bucket
    upload_file_to_s3(file)

#lambda_handler()