from aiohttp import ClientResponse, ClientResponseError, ClientSession, ServerTimeoutError
import aiohttp
from bs4 import BeautifulSoup
from multiprocessing import Lock
import asyncio
import re
from json import loads
from typing import Callable, Coroutine, Dict, List, Set, Tuple
from vendor.config.configuration import Configuration
from vendor.hhcrawler.errors.parser_errors import DatasourceExternalError, NoSearchResults
from vendor.hhcrawler.models.vacancy import Vacancy, Skill
from vendor.logging_utils import logger
from vendor.hhcrawler.utils import async_retry, retry

configuration: Configuration = Configuration({})

class Pagination:

    _current_page: int = 0
    _last_page: int = 0
    _limit: int = 0
    _executor: Callable[[int, int], Coroutine[any, any, List[Vacancy]]]
    _lock: Lock

    def __init__(self, current_page: int, max_page: int, limit: int, executor: Callable[[int, int], Coroutine[any, any, List[Vacancy]]]) -> None:
        self._current_page = current_page
        self._last_page = max_page
        self._limit = limit
        self._executor = executor
        self._lock = Lock()

    def done(self) -> bool:
        self._lock.acquire()
        result = self._current_page >= self._last_page
        self._lock.release()
        return result

    def next(self):
        if self._executor:
            self._lock.acquire()
            try:
                result = self._executor(self._current_page, self._limit)
                self._current_page += 1
                return result
            finally:
                self._lock.release()

    def last(self, page_num: int):
        self._lock.acquire()
        self._last_page = page_num
        self._lock.release()


log = logger(__name__)
employer_cache = set()


def use_configuration(conf: Configuration):
    global configuration
    configuration = conf


async def each_vacancy(search_query: str, limit: int, prefetch_size: int):
    pagination = Pagination(0, 1, prefetch_size, executor=lambda current_page,
                            prefetch_limit: _do_fetch_vacancies_using_api(search_query, prefetch_limit, current_page))
    vacancies_generator = _each_vacancy_using_api_pagination(
        pagination, limit)
    
    async for vacancy in vacancies_generator:
        yield vacancy


def _with_hh_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.82",
        "Sec-Ch-Ua": '"Not.A/Brand";v="8", "Chromium";v="114", "Microsoft Edge";v="114"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1"
    }


async def _do_fetch_vacancies_using_api(search_query: str, limit: int = 50, page: int = 0):
    async with aiohttp.ClientSession() as session:
        return await _fetch_vacancies_using_api(session, search_query, limit, page)


@async_retry(errors=[TimeoutError, ClientResponseError])
async def _fetch_vacancies_using_api(session: ClientSession, search_query: str, limit: int = 50, page: int = 0):
    request_parameters = {
        "per_page": limit,
        "text": search_query
    }
    if page:
        request_parameters["page"] = page
    url = f'{configuration.property("hh-api-endpoint")}?{"&".join( "=".join([k, "" if v is None else str(v)]) for k,v in request_parameters.items())}'
    response = await session.get(url, headers=_with_hh_headers(), timeout=configuration.property("request-timeout-in-seconds", 15))
    response.raise_for_status()
    return await response.json()


@retry(errors=[TimeoutError, NoSearchResults])
async def _each_vacancy_using_api_pagination(pagination: Pagination, limit: int):
    total_generated = 0
    async with aiohttp.ClientSession() as session:
        while not pagination.done() and total_generated < limit:
            payload = await pagination.next()

            if not payload.get("found", 0):
                raise NoSearchResults("There is no vacancies")

            last_page = payload.get("pages", 1)
            pagination.last(last_page)

            coroutines = []
            for vacancy_definition in payload.get("items", [])[0:limit]:
                if total_generated < limit:
                    coroutines.append(_fetch_and_create_vacancy_using_api(
                        session, vacancy_definition))
                    total_generated += 1
                else:
                    break

            vacancies = await asyncio.gather(*coroutines)
            for vacancy in vacancies:
                if vacancy:
                    yield vacancy
                else:
                    total_generated -= 1


@async_retry(errors=[TimeoutError, ClientResponseError])
async def _fetch_and_create_vacancy_using_api(session: ClientSession, vacancy_definition: Dict[str, any]):
    vacancy_id = vacancy_definition.get("id", None)
    vacancy_url = vacancy_definition.get("url", None)
    carrier_position = vacancy_definition.get("name", None)
    company_name = vacancy_definition.get("employer", {}).get("name", None)
    is_company_trusted = vacancy_definition.get(
        "employer", {}).get("trusted", False)

    if vacancy_url and is_company_trusted:
        response = await session.get(vacancy_url, headers=_with_hh_headers(), timeout=configuration.property("request-timeout-in-seconds", 15))
        response.raise_for_status()
        payload = await response.json()

        if company_name and carrier_position:
            skills = [Skill(name=s.get("name").lower().strip()) for s in filter(
                lambda s: True if s.get("name", False) else False, payload.get("key_skills", []))]
            salary = payload.get("salary", {})
            salary = {} if not salary else salary
            vacancy: Vacancy = Vacancy(
                company=company_name,
                description=payload.get("description", "").strip("\n").strip(),
                carrier_position=carrier_position,
                skills=skills,
                internal_id=vacancy_id,
                currency = salary.get("currency", None),
                salary_from = salary.get("from", None),
                salary_to = salary.get("to", None)
            )
            employer_details_url = payload.get("employer", {}).get("url", None)
            if employer_details_url:
                global employer_cache
                id = payload.get("employer", {}).get("id", None)
                if id and not id in employer_cache:
                    employer_details = await _fetch_employer_details(session, employer_details_url)
                    vacancy.industries = ",".join([x for x in _normalize_industries_codes(employer_details.get("industries", []))])
                    vacancy.company_site = employer_details.get("site_url", None)
                    employer_cache.add(id)
            log.info("Discovered vacancy %s", vacancy)
            return vacancy
    else:
        log.warn("Vacancy %s from %s is ignored since it is in archive or company is untrusted",
                 carrier_position, company_name)
        
def _normalize_industries_codes(industries):
    result = set()
    for x in industries:
        result.add(str(x.get("id", "0").split(".")[0]))
    return result
        
@async_retry(errors=[TimeoutError, ClientResponseError])
async def _fetch_employer_details(session: ClientSession, url: str) -> Dict[str, any]:
    response = await session.get(url, headers=_with_hh_headers(), timeout=configuration.property("request-timeout-in-seconds", 15))
    response.raise_for_status()
    return await response.json()
