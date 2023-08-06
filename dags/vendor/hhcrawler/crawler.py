import asyncio
from vendor.config.configuration import Configuration
from vendor.logging_utils import logger, time_and_log
from vendor.hhcrawler.datastore.datastore import save_all, initialize

async def _do_parse_vacancies(configuration: Configuration):
    from vendor.hhcrawler.provider.hh_dataprovider import each_vacancy
    from vendor.hhcrawler.provider.hh_dataprovider import use_configuration
    vacancies = []
    use_configuration(configuration)
    log = logger(__name__)
    try:
        async for vacancy in each_vacancy(configuration.property("vacancy-search-query", "middle python developer"),
                                         configuration.property(
                "vacancy-limit", 100),
                configuration.property(
                "vacancy-prefetch", 50)):
            vacancies.append(vacancy)
        log.info("Fetched total %s vacancies", len(vacancies))
        try:
            save_all(vacancies)
        except Exception as ex:
            log.error("Could not persist vacancies", ex)
    except Exception as ex:
        log.error("Could not fetch vacancies cause", ex)


@time_and_log
def crawler_runner(configuration: Configuration):
    initialize(configuration)
    asyncio.run(_do_parse_vacancies(configuration))