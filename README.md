Manual de uso
=================

1. El código está escrito para python 2.7x.
2. Corrí el scraper con tmux en un VPS Ubuntu 14.04 (dato actualizado).
3. Las librerías que faltan las instalé usando pip (sudo apt-get install python-pip).
4. La versión de Mongo que usé es la 2.4 instalada desde apt-get.

```
chmod +x censo_2013_completo.py
chmod +x de_mongo_a_csv.py
./censo_2013_completo.py
```
Notas:
======
* el archivo censo_2013_completo.py crea la base de datos censo_2013 y la colección censo_completo_server. Estos son los valores que le paso a de_mongo_a_csv.py
* el archivo csv tiene el cct partido en cct[10 caracteres] + id_turno. Cemabe usa ccts de 11 caracteres pero la base de escuelas activas tiene una clave de 10
```
./de_mongo_a_csv.py censo_2013 censo_completo_server_v2 censo_completo_2013.csv
```
