"""
abraChem Prospector — Base de datos persistente
Guarda todo en un archivo SQLite local: data/abrachem.db

Tablas:
    config      → API keys y preferencias (se mantienen entre sesiones)
    sesiones    → cada corrida del pipeline con su estado
    labs_queue  → cola de laboratorios pendientes de procesar
    resultados  → prospectos encontrados (se acumulan)
    logs        → historial de logs por sesión
"""

import json
from datetime import datetime
from pathlib import Path
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Config(db.Model):
    """Configuración persistente — una sola fila."""
    __tablename__ = "config"
    id              = db.Column(db.Integer, primary_key=True, default=1)
    hunter_key      = db.Column(db.String, default="")
    netrows_key     = db.Column(db.String, default="")
    max_labs        = db.Column(db.Integer, default=50)
    min_productos   = db.Column(db.Integer, default=2)
    paises_json     = db.Column(db.String, default='["ARG"]')

    @property
    def paises(self):
        return json.loads(self.paises_json or '["ARG"]')

    @paises.setter
    def paises(self, val):
        self.paises_json = json.dumps(val)

    def to_dict(self):
        return {
            "hunter_key":   self.hunter_key,
            "netrows_key":  self.netrows_key,
            "max_labs":     self.max_labs,
            "min_productos": self.min_productos,
            "paises":       self.paises,
        }


class Sesion(db.Model):
    """Una corrida del pipeline."""
    __tablename__ = "sesiones"
    id              = db.Column(db.Integer, primary_key=True)
    nombre          = db.Column(db.String)          # "ARG + CHL — 06/05/2026"
    paises_json     = db.Column(db.String)
    estado          = db.Column(db.String, default="pendiente")
                                                    # pendiente | corriendo | pausado | completado | error
    progreso        = db.Column(db.Integer, default=0)
    total           = db.Column(db.Integer, default=0)
    n_resultados    = db.Column(db.Integer, default=0)
    creada_en       = db.Column(db.DateTime, default=datetime.now)
    actualizada_en  = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    error_msg       = db.Column(db.String, default="")

    labs_queue      = db.relationship("LabQueue", backref="sesion",
                                      lazy=True, cascade="all, delete-orphan")
    resultados      = db.relationship("Resultado", backref="sesion",
                                      lazy=True, cascade="all, delete-orphan")
    logs            = db.relationship("LogEntry", backref="sesion",
                                      lazy=True, cascade="all, delete-orphan")

    @property
    def paises(self):
        return json.loads(self.paises_json or '[]')

    @property
    def pct(self):
        if self.total == 0:
            return 0
        return round(self.progreso / self.total * 100)

    def to_dict(self):
        return {
            "id":           self.id,
            "nombre":       self.nombre,
            "paises":       self.paises,
            "estado":       self.estado,
            "progreso":     self.progreso,
            "total":        self.total,
            "pct":          self.pct,
            "n_resultados": self.n_resultados,
            "creada_en":    self.creada_en.strftime("%d/%m/%Y %H:%M") if self.creada_en else "",
            "actualizada_en": self.actualizada_en.strftime("%d/%m/%Y %H:%M") if self.actualizada_en else "",
            "error_msg":    self.error_msg,
        }


class LabQueue(db.Model):
    """
    Cola de laboratorios por sesión.
    Cada laboratorio tiene un estado: pendiente | procesando | ok | error | sin_email
    Esto permite retomar desde donde se quedó.
    """
    __tablename__ = "labs_queue"
    id              = db.Column(db.Integer, primary_key=True)
    sesion_id       = db.Column(db.Integer, db.ForeignKey("sesiones.id"), nullable=False)
    pais            = db.Column(db.String)
    nombre          = db.Column(db.String)
    rubro           = db.Column(db.String)
    productos       = db.Column(db.Text)            # pipe-separated
    estado          = db.Column(db.String, default="pendiente")
                                                    # pendiente | ok | sin_email | error
    orden           = db.Column(db.Integer, default=0)

    def to_dict(self):
        return {
            "id":       self.id,
            "pais":     self.pais,
            "nombre":   self.nombre,
            "rubro":    self.rubro,
            "estado":   self.estado,
            "orden":    self.orden,
        }


class Resultado(db.Model):
    """Un prospecto encontrado."""
    __tablename__ = "resultados"
    id              = db.Column(db.Integer, primary_key=True)
    sesion_id       = db.Column(db.Integer, db.ForeignKey("sesiones.id"), nullable=False)
    pais            = db.Column(db.String)
    laboratorio     = db.Column(db.String)
    rubro           = db.Column(db.String)
    nombre          = db.Column(db.String)
    apellido        = db.Column(db.String)
    cargo           = db.Column(db.String)
    email           = db.Column(db.String)
    email_verificado = db.Column(db.String)
    fuente_email    = db.Column(db.String)
    dominio         = db.Column(db.String)
    apis_clave      = db.Column(db.Text)
    top_apis        = db.Column(db.Text)
    notas           = db.Column(db.String)
    creado_en       = db.Column(db.DateTime, default=datetime.now)

    def to_dict(self):
        return {
            "id":               self.id,
            "sesion_id":        self.sesion_id,
            "pais":             self.pais,
            "laboratorio":      self.laboratorio,
            "rubro":            self.rubro,
            "nombre":           self.nombre,
            "apellido":         self.apellido,
            "cargo":            self.cargo,
            "email":            self.email,
            "email_verificado": self.email_verificado,
            "fuente_email":     self.fuente_email,
            "dominio":          self.dominio,
            "apis_clave":       self.apis_clave,
            "top_apis":         self.top_apis,
            "notas":            self.notas,
        }


class LogEntry(db.Model):
    """Un log de una sesión."""
    __tablename__ = "logs"
    id          = db.Column(db.Integer, primary_key=True)
    sesion_id   = db.Column(db.Integer, db.ForeignKey("sesiones.id"), nullable=False)
    tipo        = db.Column(db.String)   # info | success | warning | error
    msg         = db.Column(db.Text)
    ts          = db.Column(db.String)
    creado_en   = db.Column(db.DateTime, default=datetime.now)

    def to_dict(self):
        return {
            "tipo": self.tipo,
            "msg":  self.msg,
            "ts":   self.ts,
        }
