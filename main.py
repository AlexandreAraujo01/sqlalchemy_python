from datetime import datetime
from re import S
from numpy import where
from sqlalchemy import Column, Date, Integer, Text, UniqueConstraint, create_engine, inspect, String, VARCHAR,ForeignKey, update
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


def abstract_query(db_name:Base, db_column:str, name:str = '', delete:bool = False): 
    print(db_name)
    global session
    attr = getattr(db_name, db_column)
    arr = []

    if name == '' and delete == False:
        for instance in session.query(db_name).all():
            arr.append(instance.__dict__)
            return arr
        return None
    # EXAMPLE: x = db_type.query('Fight Club')
    elif len(name) >= 1 and delete == False:
        arr = []
        for instance in session.query(db_name).filter(attr == name):
            arr.append(instance.__dict__)
        return arr

    elif delete == True and name != '':
        for instance in session.query(db_name).filter(attr == name):
            x= session.query(db_name).filter_by(**{db_column: name}).count()
            session.delete(instance)
            session.commit()
            return x 
        return None
    assert False,f"inacessivel"


def update_user_abstract(db_name,old:str,new:str):
    session.execute(
    update(db_name).
    where(db_name.first_name == old).
    values(first_name = new) 
    )
    session.commit()

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
    def append_movies(name,date,gender,author,description=''):
        formated_date = datetime.strptime(date,'%d/%m/%Y').date()
        a = Db_movies(movie_name = name, launch_date = formated_date, movie_gender = gender, movie_description = description, Author_id = author)
        session.add(a)
        session.commit()

    # function that make a query in the database based on the parameter
    def query(name = '',x = 'movie_name',delete = False):
       return abstract_query(Db_movies,x,name,delete)
        

class Author(Base):
    __tablename__ = 'Author'
    id = Column(Integer, primary_key=True)
    first_name = Column(VARCHAR(50), nullable=False)
    last_name = Column(VARCHAR(150), nullable=False)
    
    

    def append_author(name,last_name):
        print(Author.__tablename__)
        session.add(Author(first_name = name, last_name = last_name))
        session.commit()
    
    def query(name = '',delete = False):
        return abstract_query(Author, 'first_name', name, delete)

    def update_user(db_name,old,new):
        return update_user_abstract(db_name,old,new)
        

    


Author.Movies = relationship(
    "Db_movies", order_by=Db_movies.id_movie, back_populates="author"
)




# creates all customs classes as tables.
Base.metadata.create_all(bind=engine)
Session.configure(bind=engine)

# Session to use all db functionalites.
session = Session()



# ---------------------------** -------------------------------** --------------------------------------**-------------






# x = Db_movies.query('Fight Club')
# print(x)


y = Author.query('xaropinho')
print(y)


# Author.append_author('test','test')
# Author.update_user('test','xaropinho')

# Db_movies.append_movies('Fight Club', '01/10/1990','action','test')

# query = session.query(User.name, User.birthday)
# for row in query:
#     print(row._asdict())

