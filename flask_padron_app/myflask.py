# -*- coding: utf-8 -*-

from flask import Flask, jsonify
import psycopg2


app = Flask(__name__)


KEYS = [
    'ruc', 'name', 'estado', 'condicion', 'ubigeo', 'tipo_via', 'nombre_via',
    'codigo_zona', 'tipo_zona', 'numero', 'interior', 'lote', 'depa',
    'manzana', 'km'
]


def get_conn():
    try:
        conn = psycopg2.connect(
            host='localhost', port=5432, user='juan',
            password='juan', dbname='padron'
        )
        return conn.cursor()
    except psycopg2.Error as exc:
        app.logger.warning('No puedo conectarme: %s', exc)
        return jsonify({'error': True})


@app.route('/consu/<string:ruc>', methods=['GET'])
def consulta(ruc):
    cur = get_conn()
    sql_query = """
        SELECT
            ruc, name, estado, condicion, ubigeo, tipo_via,
            nombre_via, codigo_zona, tipo_zona, numero, interior,
            lote, depa, manzana, km FROM padron_reducido
        WHERE
            ruc=%s
    """
    try:
        cur.execute(sql_query, (ruc,))
    except psycopg2.ProgrammingError as exc:
        app.logger.warning('No puedo consultar SELECT * FROM padron_reducido. %s', exc)

    row = cur.fetchone()
    res = {}
    if row:
        for i in range(0, len(row)):
            try:
                res[KEYS[i]] = row[i].encode('latin-1').decode('utf-8')
            except Exception as exc:
                try:
                    res[KEYS[i]] = row[i].encode('latin-1').decode('latin-1')
                except Exception as exc:
                    res[KEYS[i]] = row[i]
        return jsonify(res)
    else:
        return jsonify({'msg': 'No encontrado'})


@app.route('/')
def hello_word():
    return 'Hellow World'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5500)
