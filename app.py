import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
from datetime import datetime
from datetime import timedelta
from scipy import stats
import pandas as pd

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(engine, reflect=True)

# Save references to each table
# Assign the demographics class to a variable called `Measurement`
Measurement = Base.classes.measurement
# Assign the demographics class to a variable called `Station`
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"For date and precip data: /api/v1.0/precipitation<br/>"
        f"For list of all stations: /api/v1.0/stations<br/>"
        f"For date and precip data: /api/v1.0/tobs<br/>"
        f"<br/>"
        f"Enter query start date in this format:<br/>"
        f"/api/v1.0/yyyy-mm-dd<br/>"
        f"Enter query start AND end date in this format:<br/>"
        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd"
    )


@app.route("/api/v1.0/precipitation")
def precip():
    # Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
    # Return the JSON representation of your dictionary.
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and precip values"""
    # Query all passengers
    results = session.query(Measurement.date, Measurement.prcp).all()

    session.close()

    # Convert list of tuples into normal list
    precip_dates = list(np.ravel(results))

    return jsonify(precip_dates)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    # Query all passengers
    results = session.query(Station.name, Station.id, Station.station, Station.latitude, Station.longitude, Station.elevation).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)


@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Design a query to find the most active station
    sel2 = [Station.name, Station.id, func.count(Station.name)]
    data_joined = session.query(*sel2).filter(Measurement.station == Station.station)\
    .group_by(Station.name).order_by(func.count(Measurement.id).desc()).limit(1).all()
    act_station = data_joined[0][1]
    
    # Find the max date and convert to date format, and get the date for one year ago from max date
    sel1 = [func.max(Measurement.date)]
    max_date_qry = session.query(*sel1).filter(Measurement.station == Station.station).filter(Station.id == act_station).all()
    max_dte = max_date_qry[0][0]
    conv_date = dt.datetime.strptime(max_dte, "%Y-%m-%d")
    one_year_ago = conv_date - dt.timedelta(weeks=52)


    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    sel3 = [Measurement.date, Measurement.tobs]
    results = session.query(*sel3).filter(Measurement.station == Station.station).filter(Station.id == act_station).filter(Measurement.date >= one_year_ago).all()

    session.close()

    # Convert list of tuples into normal list
    all_tobs = list(np.ravel(results))

    return jsonify(all_tobs)

@app.route("/api/v1.0/<start>/<end>")
def startandend(start=None, end=None):
   
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(Measurement.date, Measurement.prcp).all()

    df4 = pd.read_sql(sql = session.query(Measurement).with_entities(Measurement.date, Measurement.tobs).filter(Measurement.date>= start).filter(Measurement.date <= end).statement, 
                 con = session.bind)

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    
    tmax_temp = stats.tmax(df4['tobs'])
    tmin_temp = stats.tmin(df4['tobs'])
    tmean_temp = stats.tmean(df4['tobs'])
     
    temp_summ = [
        {'max_temp':tmax_temp},
        {'min_temp':tmin_temp},
        {'mean_temp':tmean_temp}
    ]

    return jsonify(temp_summ)

@app.route("/api/v1.0/<start>")
def startdate(start=None):
   
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(Measurement.date, Measurement.prcp).all()

    df4 = pd.read_sql(sql = session.query(Measurement).with_entities(Measurement.date, Measurement.tobs).filter(Measurement.date>= start).statement, 
                 con = session.bind)

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    
    tmax_temp = stats.tmax(df4['tobs'])
    tmin_temp = stats.tmin(df4['tobs'])
    tmean_temp = stats.tmean(df4['tobs'])
     
    temp_summ = [
        {'max_temp':tmax_temp},
        {'min_temp':tmin_temp},
        {'mean_temp':tmean_temp}
    ]

    return jsonify(temp_summ)



if __name__ == '__main__':
    app.run(debug=True)
