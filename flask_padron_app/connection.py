# -*- coding: utf-8 -*-

import psycopg2
import requests
import time
import zipfile
import csv

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_truncate_db():
    dbname = 'padron'
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            user='juan',
            password='juan',
            dbname='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT * FROM pg_database WHERE datname = %s", (dbname,))
        dbase = cur.fetchone()
        if dbase:
            cur.close()
            conn.close()
            conn2 = psycopg2.connect(
                host='localhost',
                port=5432,
                user='juan',
                password='juan',
                dbname='padron'
            )
            cur2 = conn2.cursor()
            cur2.execute('TRUNCATE padron_reducido')
            cur2.execute('drop index if exists ruc_idx')

            cur2.close()
            conn2.commit()
            return conn2
        else:
            cur.execute('CREATE DATABASE padron')
            cur.close()
            conn.close()

            conn2 = psycopg2.connect(
                host='localhost',
                port=5432,
                user='juan',
                password='juan',
                dbname='padron'
            )
            # conn2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur2 = conn2.cursor()
            cur2.execute("""
                CREATE TABLE IF NOT EXISTS padron_reducido (
                    id serial NOT NULL,
                    "name" varchar NULL, -- Nombre o Razón Social
                    ruc varchar NULL, -- RUC del contribuyente
                    estado varchar NULL, -- Estado del contribuyente
                    condicion varchar NULL, -- Condición de Domicilio
                    ubigeo varchar NULL, -- Ubigeo
                    tipo_via varchar NULL, -- Tipo vía
                    nombre_via varchar NULL, -- Nombre vía
                    codigo_zona varchar NULL, -- Código de zona
                    tipo_zona varchar NULL, -- Tipo de zona
                    numero varchar NULL, -- Número
                    interior varchar NULL, -- Interior
                    lote varchar NULL, -- Lote
                    depa varchar NULL, -- Departamento
                    manzana varchar NULL, -- Manzana
                    km varchar NULL, -- Kilómetro
                    "update" timestamp NULL, -- Fecha de actualización
                    CONSTRAINT padron_reducido_pkey PRIMARY KEY (id)
                );
            """)
            cur2.close()
            conn2.commit()
            print('Creación exitosa')
            return conn2

    except psycopg2.Error as exc:
        print('Error al crear la base de datos. %s' % exc)


def descarga_zip():
    # Descarga en partes
    url = 'http://www2.sunat.gob.pe/padron_reducido_ruc.zip'
    response = requests.get(url, stream=True)
    dstart = time.time()
    handle = open("/opt/jd/padron_reducido_ruc.zip", "wb")
    for chunk in response.iter_content(chunk_size=1024):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)
    handle.close()
    print('Tiempo de Descarga .zip: %s' % (time.time() - dstart))


def descomprimir_zip_txt():
    # Descomprimir archivo
    estart = time.time()
    zip_ref = zipfile.ZipFile("/opt/jd/padron_reducido_ruc.zip", 'r')
    zip_ref.extractall("/opt/jd/")
    zip_ref.close()
    print('Tiempo de extracción .zip -> .txt: %s' % (time.time() - estart))


def creado_csv():
    count = 0
    stime = time.time()
    csv.register_dialect('myDialect', delimiter='|', quotechar='"', escapechar='\\', quoting=csv.QUOTE_NONE)
    with open("/opt/jd/padron_reducido_ruc.txt", 'r', encoding='iso-8859-1') as input_file:
        with open("/opt/jd/padron_reducido_ruc.csv", 'w', encoding='iso-8859-1', newline='') as output_file:
            csv_reader = csv.reader(input_file, dialect='myDialect')
            writer = csv.writer(output_file, dialect='myDialect')
            for line in csv_reader:
                if count == 0:
                    pass
                else:
                    writer.writerow(line[:15])
                count += 1
        output_file.close()
        # for line in input_file:
        #     datos = line.decode('iso-8859-1').split('|')
        #     for dato in datos[:15]:
        #         # if dato in ('10702575449', '20546418694', '10462376362', '15200790704', '10075922715', '10254987421', '20546418694', '10065186476', '10413448625', '20129029642', '10076056299'):
        #         if dato in ('10076056299',):
        #             print('-----')
        #             print(line)
        #             try:
        #                 print('utf-8')
        #                 print(line.decode('iso-8859-1').encode('latin-1').decode('utf-8'))
        #             except Exception as exc:
        #                 print('latin-1')
        #                 print(line.decode('iso-8859-1').encode('latin-1').decode('latin-1'))

    input_file.close()
    print('Tiempo creado de .txt -> .csv: %s' % (time.time() - stime))


def volcado_csv_db(cr):
    # Guardado
    tvolcado = time.time()
    csv_path = "/opt/jd/padron_reducido_ruc.csv"
    csv_file = open(csv_path, 'r', encoding='latin-1')
    cr.copy_expert(
        """COPY padron_reducido(ruc, name, estado, condicion, ubigeo, tipo_via, nombre_via, codigo_zona, tipo_zona, numero, interior, lote, depa, manzana, km)
           FROM STDIN WITH DELIMITER '|'""", csv_file)
    print('Tiempo de volcado .csv -> BD: %s' % (time.time() - tvolcado))


def creado_index(cr):
    # Creado de índices para los registros, y mejorar la consulta a BD
    tindex = time.time()
    cr.execute(
        """CREATE UNIQUE INDEX ruc_idx ON padron_reducido (ruc)""")
    print('Tiempo de creado de indices: %s' % (time.time() - tindex))


def main():
    pstart = time.time()
    print('Creado/truncado de BD.')
    con = create_truncate_db()
    cur = con.cursor()

    print('Descarga de zip')
    descarga_zip()
    print('Descomprimir zip -> txt')
    descomprimir_zip_txt()
    print('Creado csv')
    creado_csv()
    print('Volcado csv -> DB')
    volcado_csv_db(cur)
    con.commit()
    print('Creado de índices.')
    creado_index(cur)
    con.commit()

    cur.close()
    con.close()
    print('Tiempo total: %s' % (time.time() - pstart))


if __name__ == '__main__':
    main()
