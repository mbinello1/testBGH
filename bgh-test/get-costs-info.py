import pandas as pd
import os
import boto3

os.chdir('c:/Users/Matias/Documents/GitHub/testBGH/bgh-test')

#primero queremos traer la info de la api del cost explorer
#damos por hecho que la info del cost explorer es del csv que pasaron

#traemos la info del csv
def get_info():
    data = pd.read_csv('cost_info.csv')
    return data

#con la info ya lista, va el codigo para hacer los calculos de cuanto gastan,
#cuanto hay que cobrar el el costo total

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

#se guarda el archivo local, borrar cuando termine de probar
info(transform_data(get_info())).to_csv('costs.csv')
file = info(transform_data(get_info())).to_csv('costs.csv')
#se guarda el archivo en el bucket
upload_file_to_s3(file)