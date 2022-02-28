from datetime import datetime
from sqlalchemy import Column, Date, Integer, Text, UniqueConstraint, create_engine, inspect, String, VARCHAR,ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import os


# Setting the Environment Variables!
os.environ['Server_mssql'] = 'DESKTOP-G8L8E1L'
os.environ['Database_mssql'] = 'pythonSQL'
os.environ['Driver_mssql'] = 'ODBC Driver 17 for SQL Server'

# MSSLQ authentification
# this login is based on Windows Authentification

SERVER = os.environ.get('Server_mssql')
DATABASE = os.environ.get('Database_mssql')
DRIVER = os.environ.get('Driver_mssql')

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


class Db_movies(Base):
    __tablename__ = 'Db_movies'
    id_movie = Column(Integer, primary_key=True)
    movie_name = Column(VARCHAR(200), unique= True)
    launch_date = Column(Date, nullable=True)
    movie_gender = Column(String, nullable=False)
    movie_description = Column(Text,nullable=True)
    Author_id = Column(Integer, ForeignKey('Author.id'))
    author = relationship("Author", back_populates="Movies")
    UniqueConstraint(movie_name)



    # function that append a movie in the database.
    # EXAMPLE: Db_movies.append_movies('Fight Club', '01/10/1990','action','test')
    def append_movies(name,date,gender,author_id,description=''):
        formated_date = datetime.strptime(date,'%d/%m/%Y').date()
        session.add(Db_movies(movie_name = name, launch_date = formated_date, movie_gender = gender, movie_description = description, author = author_id))
        session.commit()

    # function that make a query in the database based on the parameter
    def query(name = '',delete = False):
        arr = list()
        # EXAMPLE: x = Db_movies.query()
        if name == '' and delete == False:
            for instance in session.query(Db_movies).all():
                arr.append(instance.__dict__)
            return arr

        # EXAMPLE: x = Db_movies.query('Fight Club')
        elif len(name) >= 1 and delete == False:
            arr = list()
            for instance in session.query(Db_movies).filter(Db_movies.movie_name == name):
                arr.append(instance.__dict__)
            return arr

        elif delete == True and name != '':
            for instance in session.query(Db_movies).filter(Db_movies.movie_name == name):
                x= session.query(Db_movies).filter_by(movie_name=name).count()
                session.delete(instance)
                session.commit()
                return x
                

class Author(Base):
    __tablename__ = 'Author'
    id = Column(Integer, primary_key=True)
    first_name = Column(VARCHAR(50), nullable=False)
    last_name = Column(VARCHAR(50), nullable=False)

    def append_author(name,last_name):
        session.add(Author(first_name = name, last_name = last_name))
        session.commit()
    


Author.Movies = relationship(
    "Db_movies", order_by=Db_movies.id_movie, back_populates="author"
)




# creates all customs classes as tables.
Base.metadata.create_all(bind=engine)
Session.configure(bind=engine)

# Session to use all db functionalites.
session = Session()



# ---------------------------** -------------------------------** --------------------------------------**-------------


Author.append_author('Chuck','Palahniuk')

# Db_movies.append_movies('forrest gump','30/05/2014','Drama/Romance','''Slow-witted Forrest Gump (Tom Hanks) has never thought of himself as disadvantaged, and thanks to his supportive mother (Sally Field), he leads anything but a restricted life. Whether dominating on the gridiron as a college football star, fighting in Vietnam or captaining a shrimp boat, Forrest inspires people with his childlike optimism. But one person Forrest cares about most may be the most difficult to save -- his childhood love, the sweet but troubled Jenny (Robin Wright).
# ''')


# x = Db_movies.query('Fight Club')
# print(x)



# Db_movies.append_movies('Fight Club', '01/10/1990','action','test')

# query = session.query(User.name, User.birthday)
# for row in query:
#     print(row._asdict())

