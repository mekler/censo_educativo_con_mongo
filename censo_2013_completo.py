# coding=utf-8
import pycurl
import StringIO
import sys
import ast
import pymongo
from pymongo import MongoClient
def creaInfraestructura(indices, valores):
	datos = {}
	for i in range(0,48):
		datos[indices[i]] = valores[i]
	datos["Grado de rezago social"] = valores[49]
	datos["Unidad geográfica"] = valores[50]
	return datos

def construyeSQL(cadena):
	aux = cadena.split(' ')
	if len(aux)>1:
		response = '"'
		for i in range(len(aux)-1):
			response = response + 'CONTAINS( [C_BUSQUEDA], \'FORMSOF(Inflectional,'+aux[i]+')\' ) AND '
		response = response + 'CONTAINS( [C_BUSQUEDA], \'FORMSOF(Inflectional,'+aux[i+1]+')\' ) "'
	else:
		if len(aux)==1:
			response = '"CONTAINS( [C_BUSQUEDA], \'FORMSOF(Inflectional,'+cadena+')\' ) "'
	return response


def pideCenso(post_data,method):
	#url = 'http://cemabe-preliminares.inegi.org.mx/ajaxpro/CEMABE_preliminares.CEscuelas,CEMABE_preliminares.ashx'
	url = 'http://cemabe.inegi.org.mx/ajaxpro/Atlas.CEscuelas,Atlas.ashx'
	post_vars_length = 'Content-Length: ' + str(len(post_data))
	method_header = 'X-AjaxPro-Method: '+ method

	c = pycurl.Curl()
	c.setopt(pycurl.URL, url)
	c.setopt(pycurl.HTTPHEADER, ['Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language: en-US,en;q=0.5','Cache-Control: no-cache','Connection: keep-alive','Content-Type: text/plain; charset=utf-8','Host: cemabe.inegi.org.mx','Pragma: no-cache','Referer: http://cemabe.inegi.org.mx/','User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0',method_header, post_vars_length ])
	c.setopt(c.POSTFIELDS, post_data)
	import StringIO
	b = StringIO.StringIO()
	c.setopt(pycurl.WRITEFUNCTION, b.write)
	try:
		c.perform()
		response_string = b.getvalue()
		response = ast.literal_eval(response_string[:-3])
		b.close()
	except:
		response = ['error']
		client = MongoClient()
		db = client.censo_2013
		posts = db.errores
		post={
			'post_data':post_data,
			'method':method
		}
		posts.insert(post)
	return response



estados = ['aguascalientes','baja california','baja california sur', 'campeche','coahuila','colima','chiapas','chihuahua','distrito federal','durango','guanajuato','guerrero','hidalgo','jalisco','mexico','michoacan','morelos','nayarit','nuevo leon','oaxaca','puebla','queretaro','san luis potosi','sinaloa','sonora','tabasco','tamaulipas','tlaxcala','veracruz','yucatan','zacatecas']
#estados = ['baja california sur', 'campeche','coahuila','colima','chiapas','chihuahua','distrito federal','durango','guanajuato','guerrero','hidalgo','jalisco','mexico','michoacan','morelos','nayarit','nuevo leon','oaxaca','puebla','queretaro','quintana roo','san luis potosi','sinaloa','sonora','tabasco','tamaulipas','tlaxcala','veracruz','yucatan','zacatecas']
#response[0] -> KML
#response[1] -> string estructurado por parte del inegi. info parecida al kml mas datos generales
#response[2],response[3] -> lat, long de la ciudad
#response[4] -> numero de registros regresados
#response[5] -> total de registros
#response[6] -> ultimo registro regresado
for estado in estados:
	sql = construyeSQL(estado)
	base = pideCenso('{"query":'+sql+',"vent":null,"r_ini":"0","r_fin":"50"}','BusqCT')
	print base
	if base[0]!='error':
		total_registros = int(base[5])
	else:
		total_registros = 0
	ini = 0
	fin = 50
	client = MongoClient()
	db = client.censo_2013
	posts = db.cemabe_completo
	kmls = db.cemabe_kmls_completo
	i=0

	while ini<total_registros:
		print ini,fin
		response = pideCenso('{"query":'+sql+',"vent":null,"r_ini":"'+ str(ini) +'","r_fin":"'+ str(fin) +'"}','BusqCT')
		if response[0]!='error':
			cadena_inegi = response[1].split('¬¬')
			kmls.insert({'estado':estado,'pagina':i,'kml':response[0]})
			for escuela in cadena_inegi:
				if len(escuela.strip())>0:
					cadena_inegi_datos = escuela.split('*')
					if len(cadena_inegi_datos)>1:
						#cadena_inegi_datos[0] -> nombre de la escuela
						#cadena_inegi_datos[1] -> CCT
						#cadena_inegi_datos[2], cadena_inegi_datos[3] -> lat,long
						#cadena_inegi_datos[4] -> Edo 
						#cadena_inegi_datos[5] -> Municipio
						#cadena_inegi_datos[6] -> Nivel
						datos_censo = pideCenso('{"cve":"'+cadena_inegi_datos[1]+'"}','InfoWindow')
						if datos_censo[0]!='error':
							#['15 DE SEPTIEMBRE', 'PRIMARIA GENERAL', 'Censado', '455', '20', 'VIVERO TENOCHTITLAN', '12', '01/04/2014', '20270', '4499770573', '', 'Aguascalientes', 'Aguascalientes', 'Aguascalientes', 'MARIA DE LOURDES ORTIZ SIMON', 'MATUTINO', '01DPR0005R1', '', '', '11', '', 'Bajo']
							#datos_censo[0] -> nombre
							#datos_censo[1] -> tipo cct
							#datos_censo[2] -> status (censado?)
							#datos_censo[3] -> #alumnos
							#datos_censo[4] -> #personal
							#datos_censo[5] -> calle
							#datos_censo[6] -> #grupos
							#datos_censo[7] -> fecha (de publicación?)
							#datos_censo[8] -> CP
							#datos_censo[9] -> telefono
							#datos_censo[10] -> ?
							#datos_censo[11] -> Edo
							#datos_censo[12] -> Municipio
							#datos_censo[13] -> Localidad
							#datos_censo[14] -> responsable
							#datos_censo[15] -> turno
							#datos_censo[16] -> CCT
							#datos_censo[17] -> numero
							#datos_censo[18] -> ?
							#datos_censo[19] -> ? (es un numero)
							#datos_censo[20] -> ?
							#datos_censo[21] -> rezago social

							post = {'cct':cadena_inegi_datos[1],
							'nombre':cadena_inegi_datos[0],
							'coord1':cadena_inegi_datos[2],
							'coord2':cadena_inegi_datos[3],
							'edo':cadena_inegi_datos[4],
							'municipio':cadena_inegi_datos[5],
							'nivel':cadena_inegi_datos[6],
							'nombre_en_mapa':datos_censo[0],
							'tipo':datos_censo[1],
							'status':datos_censo[2],
							'num_alumnos':datos_censo[3],
							'num_personal':datos_censo[4],
							'calle':datos_censo[5],
							'num_grupos':datos_censo[6],
							'fecha':datos_censo[7],
							'cp':datos_censo[8],
							'telefono':datos_censo[9],
							'dato1':datos_censo[10],
							'edo_en_mapa':datos_censo[11],
							'municipio_en_mapa':datos_censo[12],
							'localidad_en_mapa':datos_censo[13],
							'persona_responsable':datos_censo[14],
							'turno':datos_censo[15],
							'numero_dir':datos_censo[17],
							'dato2':datos_censo[18],
							'dato3':datos_censo[19],
							'dato4':datos_censo[20],
							'rezago_social':datos_censo[21],
							}
							if post['status'].strip() == 'Censado':
								infraestructura_valores = pideCenso('{"cve":"'+cadena_inegi_datos[1]+'"}','InfraestructuraBusqueda')
								infraestructura_indices = pideCenso('{}','InfraestructuraVariables')
								if (len(infraestructura_valores)>1 and infraestructura_valores[0]!='error') and (len(infraestructura_indices)>1 and infraestructura_indices[0]!='error'):
									post['infraestructura'] = creaInfraestructura(infraestructura_indices, infraestructura_valores)
						else:
							post = {'cct':cadena_inegi_datos[0],
							'nombre':cadena_inegi_datos[1],
							'coord1':cadena_inegi_datos[2],
							'coord2':cadena_inegi_datos[3],
							'edo':cadena_inegi_datos[4],
							'municipio':cadena_inegi_datos[5],
							'nivel':cadena_inegi_datos[6],
							'error':1
							}
						try:
							posts.insert(post)
						except pymongo.errors.DuplicateKeyError,e:
							print 'Error en ',post['cct'], 'Warning', e
		ini = fin + 1
		fin = fin + 50
		i=i+1
