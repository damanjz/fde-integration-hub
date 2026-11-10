"""Settings. Env prefix HUB_, nested with double underscore (HUB_WAREHOUSE__CLIENT_SECRET)."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent


class OAuthDestination(BaseModel):
    base_url: str
    token_path: str = "/oauth/token"
    send_path: str
    lookup_path: str | None = None
    lookup_param: str = "reference"
    idempotency_header: bool = False  # send the delivery key as Idempotency-Key
    duplicate_statuses: list[int] = []  # statuses that may mean "already have it"
    duplicate_error: str | None = None  # ... but only if the body's `error` equals this ...
    duplicate_number_field: str | None = None  # ... and this body field equals the delivery key
    idempotency_ttl_s: float | None = None
    client_id: str = ""
    client_secret: SecretStr = SecretStr("")
    rate: float = 4.0  # requests per second, kept below the provider limit on purpose
    burst: float = 2.0
    timeout_s: float = 10.0


class ApiKeyDestination(BaseModel):
    base_url: str
    send_path: str
    lookup_path: str | None = None
    lookup_param: str = "reference"
    idempotency_header: bool = False
    duplicate_statuses: list[int] = []
    duplicate_error: str | None = None
    duplicate_number_field: str | None = None
    idempotency_ttl_s: float | None = None  # how long the provider remembers an Idempotency-Key
    api_key: SecretStr = SecretStr("")
    api_key_header: str = "X-Api-Key"
    rate: float = 20.0
    burst: float = 10.0
    timeout_s: float = 10.0


class WarehouseDestination(OAuthDestination):
    """Warehouse (3PL): no idempotency support, so the hub asks by external_ref before resending."""

    base_url: str = "http://localhost:9000/warehouse"
    send_path: str = "/shipments"
    lookup_path: str | None = "/shipments"
    lookup_param: str = "external_ref"
    client_id: str = "hub-warehouse"
    client_secret: SecretStr = SecretStr("dev-warehouse-secret")
    rate: float = 4.0  # provider limit is 5 req/s
    burst: float = 2.0


class HelpdeskDestination(ApiKeyDestination):
    """Helpdesk: honours Idempotency-Key for 24 h, so the delivery key is sent as that header."""

    base_url: str = "http://localhost:9000/helpdesk"
    send_path: str = "/v1/records"
    idempotency_header: bool = True
    idempotency_ttl_s: float | None = 24 * 3600
    api_key: SecretStr = SecretStr("dev-helpdesk-key")


class AccountingDestination(OAuthDestination):
    """Accounting: invoice numbers are unique; 409 means we already booked it, which counts as delivered."""

    base_url: str = "http://localhost:9000/accounting"
    send_path: str = "/v1/documents"
    duplicate_statuses: list[int] = [409]
    duplicate_error: str | None = "duplicate_document_number"
    duplicate_number_field: str | None = "number"
    client_id: str = "hub-accounting"
    client_secret: SecretStr = SecretStr("dev-accounting-secret")
    rate: float = 10.0
    burst: float = 5.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HUB_", env_nested_delimiter="__", env_file=str(ROOT / ".env"), extra="ignore"
    )

    database_url: str = (
        ""  # required: postgresql://hub:<HUB_DB_PASSWORD>@127.0.0.1:5434/hub (see .env.example)
    )
    webhook_secrets: SecretStr = SecretStr("")  # comma separated, first is current
    webhook_window_s: int = 300
    webhook_max_body: int = 1_000_000
    admin_key: SecretStr = SecretStr("")  # empty disables the admin endpoints

    # Delivery policy.
    max_attempts: int = 10  # failed cycles (429 excluded) before a delivery is dead-lettered
    # Hard stop including uncounted cycles (429, failures while the breaker is open) and crashes. At the
    # 30 s breaker cooldown, 500 cycles ride out a destination outage of about four hours.
    max_total_attempts: int = 500
    backoff_base_s: float = 1.0
    backoff_cap_s: float = 30.0
    lease_s: float = 60.0  # a claimed row is re-claimable (and reconciled) after this
    max_inline_wait_s: float = 2.0  # longer gate waits release the row instead of holding it
    breaker_threshold: int = 5
    breaker_base_s: float = 2.0
    breaker_cap_s: float = 30.0
    max_retry_after_s: float = 120.0
    # After a timeout or reset (no response) the request may still be running at the destination: wait this
    # long before looking it up, so the lookup is unlikely to race the original request.
    ambiguous_settle_s: float = 3.0

    # Workers.
    workers: int = 4  # embedded in the API process; 0 to run workers separately
    poll_interval_s: float = 0.25
    pool_size: int = 20

    # Test-only fault injection: kill a worker after the destination accepted, before success is recorded.
    chaos_crash_rate: float = 0.0

    warehouse: WarehouseDestination = WarehouseDestination()
    helpdesk: HelpdeskDestination = HelpdeskDestination()
    accounting: AccountingDestination = AccountingDestination()

    def secrets_list(self) -> list[str]:
        return [s.strip() for s in self.webhook_secrets.get_secret_value().split(",") if s.strip()]

    def all_secret_values(self) -> list[str]:
        """Every configured secret, for log and error-body redaction."""
        values = [
            *self.secrets_list(),
            self.admin_key.get_secret_value(),
            self.helpdesk.api_key.get_secret_value(),
            self.warehouse.client_secret.get_secret_value(),
            self.accounting.client_secret.get_secret_value(),
        ]
        return [v for v in values if len(v) >= 4]


def load_settings() -> Settings:
    return Settings()
