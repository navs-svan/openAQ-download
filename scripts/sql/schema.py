from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import TEXT, INTEGER, NUMERIC, TIMESTAMP
from sqlalchemy.orm import declarative_base, relationship

from sqlalchemy import UniqueConstraint


Base = declarative_base()


class Countries(Base):
    __tablename__ = "countries"

    country_id = Column(INTEGER, primary_key=True)
    country_name = Column(TEXT)
    first_date = Column(TIMESTAMP)
    last_date = Column(TIMESTAMP)

    location = relationship("Locations", back_populates="country")


class Locations(Base):
    __tablename__ = "locations"

    location_id = Column(INTEGER, primary_key=True)
    location_name = Column(TEXT)
    latitude = Column(NUMERIC)
    longitude = Column(NUMERIC)
    first_date = Column(TIMESTAMP)
    last_date = Column(TIMESTAMP)
    country_id = Column(INTEGER, ForeignKey(Countries.country_id))

    country = relationship("Countries", back_populates="location", lazy="joined")
    measure = relationship("Measurements", back_populates="location")


class Parameters(Base):
    __tablename__ = "parameters"

    parameter_id = Column(INTEGER, primary_key=True)
    parameter_name = Column(TEXT)
    units = Column(TEXT)
    display_name = Column(TEXT)
    description = Column(TEXT)

    sensor = relationship("Sensors", back_populates="parameter")


class Sensors(Base):
    __tablename__ = "sensors"

    sensor_id = Column(INTEGER, primary_key=True)
    parameter_id = Column(INTEGER, ForeignKey(Parameters.parameter_id))

    parameter = relationship("Parameters", back_populates="sensor", lazy="joined")
    measure = relationship("Measurements", back_populates="sensor")


class Measurements(Base):
    __tablename__ = "measurements"

    location_id = Column(INTEGER, ForeignKey(Locations.location_id), primary_key=True)
    sensors_id = Column(INTEGER, ForeignKey(Sensors.sensor_id), primary_key=True)
    datetime = Column(TIMESTAMP, primary_key=True)
    value = Column(NUMERIC)

    UniqueConstraint(location_id, sensors_id, datetime, name="measure_uix")

    def __init__(self, location, sensor, datetime, value):
        self.location = location
        self.sensor = sensor
        self.datetime = datetime
        self.value = value

    location = relationship("Locations", back_populates="measure", lazy="joined")
    sensor = relationship("Sensors", back_populates="measure", lazy="joined")
