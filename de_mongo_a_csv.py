#!/usr/bin/python
# coding=utf-8
#./de_mongo_a_csv.py censo_2013 censo_completo_server_v2 censo_completo_2013.csv
import pymongo
from pymongo import MongoClient
import csv
import sys
import codecs

db_name = sys.argv[1]
col_name = sys.argv[2]

aux = sys.argv[1]
client = MongoClient()
collection = client[db_name][col_name]

with open(sys.argv[3], 'wb') as csvfile:
	spamwriter = csv.writer(csvfile, delimiter='|',
		quotechar='"', quoting=csv.QUOTE_MINIMAL)
	for post in collection.find():
		if 'edo_en_mapa' in post:
			if post['num_grupos']=='':
				post['num_grupos'] = '0'
			if 'infraestructura' in post:
				renglon = [post['edo_en_mapa'],\
				post['num_alumnos'],\
				post['persona_responsable'],\
				post['nombre_en_mapa'],\
				post['num_grupos'],\
				post['edo'],\
				post['cp'],\
				post['tipo'],\
				post['fecha'],\
				post['dato4'],\
				post['dato3'],\
				post['coord2'],\
				post['dato1'],\
				post['nombre'],\
				post['telefono'],\
				post['localidad_en_mapa'],\
				post['dato2'],\
				post['status'],\
				post['nivel'],\
				post['coord1'],\
				post['municipio_en_mapa'],\
				post['rezago_social'],\
				post['numero_dir'],\
				post['num_personal'],\
				post['municipio'],\
				post['cct'][0:10],\
				post['calle'],\
				post['id_turno'][-1],\
				post['turno'],\
				post['infraestructura']['Servicio de internet'],\
				post['infraestructura']['Programa Escuela Siempre Abierta'],\
				post['infraestructura']['Cooperativa, cafetería o tienda escolar'.decode('utf-8')],\
				post['infraestructura']['Enfermería o servicio médico'.decode('utf-8')],\
				post['infraestructura']['Programa Desayunos Escolares'],\
				post['infraestructura']['Zonas de seguridad'],\
				post['infraestructura']['Escuelas de Tiempo Completo'],\
				post['infraestructura']['Biblioteca'],\
				post['infraestructura']['Habilidades Digitales para Todos'],\
				post['infraestructura']['Aulas de usos múltiples'.decode('utf-8')],\
				post['infraestructura']['Cuartos para baños o sanitarios'.decode('utf-8')],\
				post['infraestructura']['Programa Nacional de Inglés en Educación Básica'.decode('utf-8')],\
				post['infraestructura']['Tazas sanitarias'],\
				post['infraestructura']['Letrina u hoyo negro'],\
				post['infraestructura']['Servicio de agua de la red pública'.decode('utf-8')],\
				post['infraestructura']['Programa Asesor Técnico Pedagógico y para la Atención Educativa a la Diversidad Social Lingüística y Cultural'.decode('utf-8')],\
				post['infraestructura']['Drenaje'],\
				post['infraestructura']['Mingitorios'],\
				post['infraestructura']['Energía eléctrica'.decode('utf-8')],\
				post['infraestructura']['Áreas deportivas y recreativas'.decode('utf-8')],\
				post['infraestructura']['Programa Ver Bien para Aprender Mejor'],\
				post['infraestructura']['Programa de Educación Primaria para Niñas y Niños Migrantes'.decode('utf-8')],\
				post['infraestructura']['Salidas de emergencia'],\
				post['infraestructura']['Patio o plaza cívica'.decode('utf-8')],\
				post['infraestructura']['Aulas de cómputo'.decode('utf-8')],\
				post['infraestructura']['Aulas para talleres'],\
				post['infraestructura']['Asociación de padres de familia'.decode('utf-8')],\
				post['infraestructura']['Fortalecimiento del Servicio de la Educación Telesecundaria'.decode('utf-8')],\
				post['infraestructura']['Proyecto Mejoramiento del Logro Educativo en Escuelas Primarias Multigrado'],\
				post['infraestructura']['Programa de Infraestructura "Mejores Escuelas"'],\
				post['infraestructura']['Grado de rezago social'],\
				post['infraestructura']['Lavamanos'],\
				post['infraestructura']['Aulas para impartir clase'],\
				post['infraestructura']['Enciclomedia'],\
				post['infraestructura']['Programa Escuela Segura'],\
				post['infraestructura']['Oportunidades'],\
				post['infraestructura']['Consejo de participación social'.decode('utf-8')],\
				post['infraestructura']['Rutas de evacuación'.decode('utf-8')],\
				post['infraestructura']['Escuelas de Bajo Rendimiento'],\
				post['infraestructura']['Programa de Educación Inicial y Básica para la Población Rural e Indígena, antes FIDUCAR'.decode('utf-8')],\
				post['infraestructura']['Programa Fortalecimiento de la Educación Especial y de la Integración Educativa'.decode('utf-8')],\
				post['infraestructura']['Programa Nacional de Lectura'],\
				post['infraestructura']['Teléfono'.decode('utf-8')],\
				post['infraestructura']['Unidad geográfica'.decode('utf-8')],\
				post['infraestructura']['Programa Emergente para la Mejora del Logro Educativo'],\
				post['infraestructura']['Laboratorios'],\
				post['infraestructura']['Señales de protección civil'.decode('utf-8')],\
				post['infraestructura']['Programa de Acciones Compensatorias para Abatir el Rezago Educativo en Educación Inicial y Básica'.decode('utf-8')],\
				post['infraestructura']['Cisterna o aljibe'],\
				post['infraestructura']['Programa Escuelas de Calidad']]
			else:
				#if post['municipio'].find(u'\xfa')>=0:
				#	print post['municipio']
				#	sys.exit()
				renglon = [post['edo_en_mapa'],\
				post['num_alumnos'],\
				post['persona_responsable'],\
				post['nombre_en_mapa'],\
				post['num_grupos'],\
				post['edo'],\
				post['cp'],\
				post['tipo'],\
				post['fecha'],\
				post['dato4'],\
				post['dato3'],\
				post['coord2'],\
				post['dato1'],\
				post['nombre'],\
				post['telefono'],\
				post['localidad_en_mapa'],\
				post['dato2'],\
				post['status'],\
				post['nivel'],\
				post['coord1'],\
				post['municipio_en_mapa'],\
				post['rezago_social'],\
				post['numero_dir'],\
				post['num_personal'],\
				post['municipio'],\
				post['cct'][0:10],\
				post['calle'],\
				post['id_turno'][-1],\
				post['turno']]
		else:
			renglon = [	post['edo'],\
			post['coord2'],\
			post['nombre'],\
			post['nivel'],\
			post['coord1'],\
			post['municipio'],\
			post['cct']]
		k = len(renglon)
		for i in range(0,k):
			try:
				renglon[i] = renglon[i].encode('utf-8')
			except:
				renglon[i] = ''
		if k != 79:
			if k == 29:
				for i in range (k,79):
					renglon.append('')
			else:
				print "renglon:" +str(k)
				print len(post), len(renglon)
				sys.exit()
		spamwriter.writerow(renglon)