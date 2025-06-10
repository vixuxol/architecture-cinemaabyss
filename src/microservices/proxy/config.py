import re
import os

from env import get_str_var, get_int_var, get_bool_var


_DEFAULT_SERVICE_HOST = "0.0.0.0"
_DEFAULT_SERVICE_PORT = 8000


class AppConfig:
    def __init__(self) -> None:
        self.host = get_str_var("HOST", _DEFAULT_SERVICE_HOST)
        self.port = get_int_var("PORT", _DEFAULT_SERVICE_PORT)

        self.monolith_service = get_str_var("MONOLITH_URL")
        self.gradual_migration = get_bool_var("GRADUAL_MIGRATION", False)
        self.migration_config = None
        if self.gradual_migration:
            self.migration_config = self.__get_migration_config()

    def __get_migration_config(self) -> dict[str, dict[str, str | int]]:
        pattern = re.compile(r"^(\w+)_SERVICE_URL$")
        service_urls = {}

        for key, value in os.environ.items():
            match = pattern.match(key)
            if match:
                service_name_upper = match.group(1)

                service_migration_percent = get_int_var(f"{service_name_upper}_MIGRATION_PERCENT", 0)
                service_name = service_name_upper.lower()
                service_urls[service_name] = {
                    "microservice_url": value,
                    "migration_percent": service_migration_percent,
                }

        return service_urls

    def __str__(self):
        return (
            f"API Gateway host: {self.host}.\n" f"API Gateway port: {self.port}.\n" "GRADUAL MIGRATION " + "is disabled"
            if not self.gradual_migration
            else "is enabled" + ".\n" f"Migration config: {self.migration_config}"
        )
