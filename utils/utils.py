import psycopg2
from utils.config import config
from classes.HHParser import HHParser


def create_database(db_name):
    conn = psycopg2.connect(dbname="postgres", **config())
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    cur.close()
    conn.close()


def create_tables(db_name):
    conn = psycopg2.connect(dbname="db_name", **config())
    with conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE employers 
                           (employer_id int PRIMARY KEY,
                           name VARCHAR(255) UNIQUE NOT NULL,
                           open_vacancies int
                           );
                           ''')
            cur.execute('''CREATE TABLE vacancies
                        (vacancy_id serial,
                        name VARCHAR(255) NOT NULL,
                        salary_from int,
                        salary_to int,
                        url VARCHAR(255),
                        employer_id int REFERENCES employers(employer_id) NOT NULL
                        );
                        ''')
    conn.close()


def insert_data_into_tables(db_name):
    hh = HHParser()
    employers = hh.get_employers()
    vacancies = hh.filter_vacancies()
    conn = psycopg2.connect(dbname='db_name', **config())
    with conn:
        with conn.cursor() as cur:
            for employer in employers:
                try:
                    cur.execute("""
                                INSERT INTO employers VALUES (%s, %s, %s);
                                """, (employer['id'], employer['name'], employer['open_vacancies']))
                except KeyError:
                    open_vacancies = None

            for vacancy in vacancies:
                try:
                    cur.execute("""
                                INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s);
                                """, (vacancy['vacancy_id'], vacancy['name'],
                                vacancy['salary_from'], vacancy['salary_from'],
                                vacancy['url'], vacancy['id']))
                except KeyError:
                    vacancy_id = None
    conn.close()

create_database("db_name")
create_tables("db_name")
insert_data_into_tables("db_name")
