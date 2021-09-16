from .abc import DiscoveryClient


class KongDiscovery(DiscoveryClient):
    async def subscribe(
        self,
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        retry_tries: int = 3,
        retry_delay: float = 5,
    ) -> None:
        endpoint = f"{self.route}/services"
        service_metadata = {
            "name": name,
            "url": f"http://{host}:{port}",
        }
        await self._rest_subscribe(endpoint, service_metadata, host, port, name, endpoints, retry_tries, retry_delay)

        endpoint = f"{self.route}/{name}/routes"
        service_metadata = {
            "paths": [endpoint["url"] for endpoint in endpoints],
        }
        # TODO Should we use _rest_subscribe here?
        await self._rest_subscribe(endpoint, service_metadata, host, port, name, endpoints, retry_tries, retry_delay)

    async def unsubscribe(self, name: str, retry_tries: int = 3, retry_delay: float = 5) -> None:
        endpoint = f"{self.route}/services/{name}"

        await self._rest_unsubscribe(endpoint, name, retry_tries, retry_delay)
