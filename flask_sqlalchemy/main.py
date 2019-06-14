# -*- coding: utf-8 -*-

from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)

engine = create_engine('mysql+pymysql://USUARIO:PASSWORD@HOST:PORT/NOMBREDB')
Base = declarative_base(engine)


class GeneralPersona(Base):
    __tablename__ = 'general.persona'
    __table_args__ = {'autoload': True}


class EmisionEmisor(Base):
    __tablename__ = 'emision.emisor'
    __table_args__ = {'autoload': True}


class EmisorDocElectronicoConf(Base):
    __tablename__ = 'facturacion.emisor_documento_electronico_configuracion'
    __table_args__ = {'autoload': True}


class DocElectronicoConf(Base):
    __tablename__ = 'facturacion.documento_electronico_configuracion'
    __table_args__ = {'autoload': True}


def load_session():
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


@app.route('/')
def index():
    data = []
    return render_template('index.html', datas=data)


@app.route('/buscar', methods=['POST'])
def buscar():
    if request.method == 'POST':
        session = load_session()
        ruc = request.form['ruc']
        if len(ruc) == 11:
            res = session.query(GeneralPersona).filter(GeneralPersona.numDocPrincipal == ruc).all()
            if len(res) > 0:
                id_persona = res[0].idPersona
                res = session.query(EmisorDocElectronicoConf)\
                    .filter(EmisorDocElectronicoConf.idEmisor == id_persona).all()
                res_em = session.query(EmisionEmisor.aliasUrl).all()
            return render_template('index.html', datas=res)


@app.route('/edit/<int:emisor>/<string:tipo_doc>/<string:cod_agencia>/<string:cod_conf>', methods=['POST', 'GET'])
def edit(emisor=0, tipo_doc='00', cod_agencia='000', cod_conf='0000'):
    session = load_session()
    res = session.query(EmisorDocElectronicoConf)\
        .filter(EmisorDocElectronicoConf.idEmisor == emisor)\
        .filter(EmisorDocElectronicoConf.tipoDocumento == tipo_doc)\
        .filter(EmisorDocElectronicoConf.codAgencia == cod_agencia)\
        .filter(EmisorDocElectronicoConf.codConfiguracion == cod_conf).all()
    if len(res):
        return render_template('edit_conf.html', data=res[0])


@app.route('/update', methods=['POST'])
def update():
    if request.method == 'POST':
        emisor = request.form['emisor']
        tipo_doc = request.form['tipo_doc']
        cod_agencia = request.form['cod_agencia']
        cod_conf = request.form['cod_conf']
        valor = request.form['valor']

        session = load_session()
        res = session.query(EmisorDocElectronicoConf)\
            .filter(EmisorDocElectronicoConf.idEmisor == emisor)\
            .filter(EmisorDocElectronicoConf.tipoDocumento == tipo_doc)\
            .filter(EmisorDocElectronicoConf.codAgencia == cod_agencia)\
            .filter(EmisorDocElectronicoConf.codConfiguracion == cod_conf)\
            .update({EmisorDocElectronicoConf.valor: valor},
                    synchronize_session=False)
        session.commit()
        res = session.query(EmisorDocElectronicoConf).filter(
            EmisorDocElectronicoConf.idEmisor == emisor).all()
        return render_template('index.html', datas=res)


if __name__ == '__main__':
    app.run(port=3000, debug=True)
