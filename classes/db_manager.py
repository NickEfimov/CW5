import psycopg2
from utils import config


class DBManager:

    def get_companies_and_vacancies_count(self):
        ''' Метод, получающий список всех компаний и вакансий у каждой компании. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT company_name, count(vacancies_name) AS count_vacancies FROM employers JOIN vacancies using (employer_id) GROUP BY employers.company_name')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_all_vacancies(self):
        ''' Метод, получающий список всех вакансий. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT employers.company_name, vacancies.vacancies_name, vacancies.payment, vacancies_url FROM employers JOIN vacancies using (employer_id)')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_avg_salary(self):
        ''' Метод, получающий среднюю зарплату по вакансиям. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT AVG(payment) AS payment_avg FROM vacancies')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_higher_salary(self):
        ''' Метод, получающий список всех вакансий, у которых зарплата выше средней по всем вакансиям. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM vacancies WHERE payment > (select AVG(payment) FROM vacancies)')
                result = cur.fetchall()
            conn.commit()
        return result
    def get_vacancies_with_keyword(self, keywords):
        ''' Метод, поиска всех вакансий по ключевому слову. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM vacancies WHERE vacancies_name LIKE \'%{keywords}%\'')
                result = cur.fetchall()
            conn.commit()
        return result