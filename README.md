# testBGH
En la carpeta hay 3 archivos, el csv original de donde salen los datos, el codigo y un csv con el resultado.

El codigo busca el csv original en el directorio local pero esa parte se podria saltear si se incorpora el codigo del api call para traer el archivo directamente
sin tener que guardarlo en algun lugar previo.

Una vez que estan los datos, se hacen transformaciones para obtener los datos en los formatos correspondientes y guardarlo en un nuevo csv (costs.csv)

Esos datos son luego subidos a un bucket de S3 (deje datos para completar como el nombre del bucket al que hay que cargar la info).
Como el codigo se estaria corriendo desde un entorno de AWS, no hace falta ingresar credenciales o usar el secret manager para resguardar las credenciales para 
acceder al bucket (suponiendo que se asignen los roles correspondientes a la lambda que ejecutaria el codigo).

Con respecto a la Lambda, es necesario agregarle una layer (AWS proporciona una, no hay que cargar una a mano) para poder importar y usar Pandas en el codigo
