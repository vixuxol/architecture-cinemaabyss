from service import EventsService
from config import AppConfig
from init_logging import init_logging


def main() -> None:
    app_config = AppConfig()
    logger = init_logging("EventsService")
    srv = EventsService(config=app_config, logger=logger)
    srv.start()


if __name__ == "__main__":
    main()
