from datetime import datetime
from sqlalchemy import Column, Date, Integer, Text, UniqueConstraint, create_engine, inspect, String, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

os.environ['Server_mssql'] = ''
os.environ['Database_mssql'] = ''
os.environ['Driver_mssql'] = ''

# MSSLQ authentification
# this login is based on Windows Authentification
SERVER ='DESKTOP-G8L8E1L'
DATABASE = 'pythonSQL'
DRIVER = 'ODBC Driver 17 for SQL Server'

# to log using login and password follow the documentation bellow.
# https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pymssql


# estabilsh connection with Database
Database_con = f'mssql://@{SERVER}/{DATABASE}?driver={DRIVER}'
engine=create_engine(Database_con)


# Default class to shape the  custom classes
Base = declarative_base()
# create a variable to use the databse
Session = sessionmaker()



# tables on database
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(50), nullable=False)
    UniqueConstraint(name)

class Db_movies(Base):
    __tablename__ = 'Db_movies'
    id_movie = Column(Integer, primary_key=True)
    movie_name = Column(VARCHAR(200), unique= True)
    launch_date = Column(Date, nullable=True)
    movie_gender = Column(String, nullable=False)
    movie_description = Column(Text,nullable=True)
    UniqueConstraint(movie_name)
    # function that append a movie in the database.
    def append_movies(name,date,gender,description=''):
        formated_date = datetime.strptime(date, '%d/%m/%Y').date()
        session.add(Db_movies(movie_name = name, launch_date = formated_date, movie_gender = gender, movie_description = description))
        session.commit()

    def query(movie_name = ''):
        if movie_name == '':
            for instance in session.query(Db_movies).all():
                print(f'Id = {instance.id_movie}, Name = {instance.movie_name}, Launch Date = {instance.launch_date}, Movie Gender = {instance.movie_gender}, Description = {instance.movie_description}')
        elif len(movie_name) >= 1:
            for instance in session.query(Db_movies).filter(Db_movies.movie_name == movie_name):
                dic = {'id': instance.id_movie, 'name': instance.movie_name, 'launch_date': instance.launch_date, 'gender': instance.movie_gender, 'description': instance.movie_description}
                print(dic)


    


# creates all customs classes as tables.
Base.metadata.create_all(bind=engine)
Session.configure(bind=engine)

# Session to use all db functionalites.
session = Session()


# ---------------------------** -------------------------------** --------------------------------------**-------------

# Db_movies.append_movies('forrest gump','30/05/2014','Drama/Romance','''Slow-witted Forrest Gump (Tom Hanks) has never thought of himself as disadvantaged, and thanks to his supportive mother (Sally Field), he leads anything but a restricted life. Whether dominating on the gridiron as a college football star, fighting in Vietnam or captaining a shrimp boat, Forrest inspires people with his childlike optimism. But one person Forrest cares about most may be the most difficult to save -- his childhood love, the sweet but troubled Jenny (Robin Wright).
# ''')


Db_movies.query()

# session.add(db_movies(id_movie='Fight Club', launch_date =dt.date(1990, 10, 1),movie_gender = 'action', movie_description = 'test'))
# session.commit()

# Db_movies.append_movies('Fight Club', '1990-10-01','action','test')

# query = session.query(User.name, User.birthday)
# for row in query:
#     print(row._asdict())

