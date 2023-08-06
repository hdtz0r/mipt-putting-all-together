from typing import List, Tuple
import pendulum

from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook


class HeadHunterCrawler(BaseOperator):

    def __init__(self, config_filename: str, **kwargs):
        super().__init__(**kwargs)
        self.config_filename = config_filename

    def execute(self, context: Context):
        from vendor.config.provider import ConfigurationProvider
        from vendor.hhcrawler.crawler import crawler_runner
        crawler_runner(ConfigurationProvider().load(self.config_filename))

class HttpxFileDownloader(BaseOperator):

    def __init__(self, config_filename: str, **kwargs):
        super().__init__(**kwargs)
        self.config_filename = config_filename

    def execute(self, context: Context):
        from vendor.config.provider import ConfigurationProvider
        from vendor.downloader.downloader import download
        configuration = ConfigurationProvider().load(self.config_filename)
        download(configuration.property("url"), configuration.property("output-filename"))

def process_batch(executor):
        executor.execute()

vacancies_dataset = Dataset("postgresql://vacancies")
companies_dataset = Dataset("postgresql://companies")
start_date = pendulum.now('UTC')
    
@dag(
    schedule="0 */6 * * *",
    start_date=start_date,
    catchup=False,
    tags=["hw", "producer", "vacancies"],
    max_active_runs=1
)
def discover_hh_relevant_vacancies():
    HeadHunterCrawler(task_id = "search-relevant-vacancies", config_filename = "./config/hh-crawler-settings.yml", outlets=[vacancies_dataset])
    
@dag(
    schedule="@monthly",
    start_date=start_date,
    catchup=False,
    tags=["hw", "producer", "companies"],
    max_active_runs=1
)
def discover_relevant_companies():
    
    @task(task_id="discover-telecom-companies")
    def discover_telecom_companies():
        from vendor.companies.producer import process_company_registry
        from vendor.config.provider import ConfigurationProvider
        executors = []
        for executor in process_company_registry(ConfigurationProvider().load("./config/companies-producer-settings.yml")):
            executors.append(executor)
        return executors
    
    @task
    def process_telecom_companies(executor):
        process_batch(executor)

    executors = HttpxFileDownloader(task_id = "download-company-registry", config_filename = "./config/company-registry-downloader.yml") >> discover_telecom_companies()
    process_telecom_companies.expand(executor=executors) >> EmptyOperator(task_id = "post-process-companies", outlets=[companies_dataset])

@dag(
    schedule=[vacancies_dataset],
    start_date=start_date,
    catchup=False,
    tags=["hw", "consumer", "rating"],
)
def search_top_skills():

    @task(task_id="search-telecom-vacancies")
    def search_telecom_vacancies():
        postgres_hook = PostgresHook(postgres_conn_id='hh_vacancies_store')
        with postgres_hook.get_conn().cursor() as cursor:
            cursor.execute("""
                SELECT
                    S.NAME,
                    COUNT(V.ID) AS TOTAL
                FROM
                    VACANCY V
                INNER JOIN SKILL S ON
                    S.VACANCY_ID = V.ID
                WHERE
                    '9' = ANY (STRING_TO_ARRAY(V.INDUSTRIES,
                    ',')) -- 9 means telecom
                GROUP BY
                    S.NAME
                ORDER BY
                    TOTAL DESC          
            """)
            records = cursor.fetchall()
            print(records)
            return records
        

    @task(task_id = "analyze-telecom-companies")
    def analyze_telecom_companies(skills_using_industry_meta: List[Tuple[str, int]]):
        result_set = {}
        for skill in skills_using_industry_meta:
            name, total_vacanies = skill
            result_set[name] = total_vacanies

        vacancies_store = PostgresHook(postgres_conn_id='hh_vacancies_store')
        with vacancies_store.get_conn().cursor() as vacancies_cursor:
            vacancies_cursor.execute("""
                SELECT
                    V.ID,
                    V.COMPANY
                FROM
                    VACANCY V     
            """)
            records = vacancies_cursor.fetchall()
            if records:
                companies_store = PostgresHook(postgres_conn_id='companies_store')
                relevant_vacancy_ids = []
                with companies_store.get_conn().cursor() as companies_cursor: 
                    for r in records:
                        id, name = r
                        if name:
                            companies_cursor.execute("""
                                SELECT COUNT(ID) > 0 FROM COMPANY C 
                                WHERE STRPOS(LOWER(C.NAME), %s) > 0
                            """, (name.lower().strip(),))
                            exists, = companies_cursor.fetchone()
                            if exists:
                                relevant_vacancy_ids.append(id)


                vacancies_cursor.execute("""
                    SELECT
                        S.NAME,
                        COUNT(V.ID) AS TOTAL
                    FROM
                        VACANCY V
                    INNER JOIN SKILL S ON
                        S.VACANCY_ID = V.ID
                    WHERE
                        V.ID IN %s
                    GROUP BY
                        S.NAME
                    ORDER BY
                        TOTAL DESC          
                """, (tuple(relevant_vacancy_ids),))

                skill_set = vacancies_cursor.fetchall()

                for skill in skill_set:
                    skill, total_vacanies = skill
                    result_set[skill] = result_set.get(skill, 0) + total_vacanies

        return result_set
    

    @task(task_id = "write-hard-skill-rating")
    def write_results(r):
        output_filename = Variable.get("skill_rating_output_file", default_var="/opt/airflow/external-data/hh-python-dev-skill-rating.csv")
        _write_csv(output_filename, ["skill", "total_vacancies"], [[k,v] for k,v in r.items()])
    

    def _write_csv(output_file: str, headers, rows):
        import csv
        with open(output_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(headers)
            csv_writer.writerows(rows)


    write_results(analyze_telecom_companies(search_telecom_vacancies())) 


discover_hh_relevant_vacancies()
discover_relevant_companies()
search_top_skills()