# pipeline/constants.py
#
# Single source of truth for all hardcoded values in the pipeline.
# Operator-configurable values live in config.py; physics-of-the-system
# values (costs, delays, protocol constants) live here.

# --- API cost per call (USD) ---
API_COSTS: dict[str, float] = {
    "serper": 0.001,
    "brave": 0.001,
    "zuhal": 0.0005,
}

# --- Exponential backoff parameters (base_seconds, max_seconds) per service ---
SERVICE_BACKOFF: dict[str, tuple[float, float]] = {
    "dns": (0.5, 8.0),
    "serper": (1.0, 32.0),
    "brave": (1.0, 32.0),
    "zuhal": (1.0, 64.0),
}

# --- DNS ---
DNS_TLDS: tuple[str, ...] = (".com", ".net", ".org")
DOMAIN_STEM_MIN_LENGTH: int = 5  # short stems (e.g. "php", "nsg") match unrelated registered domains
MAX_WITHOUT_CANDIDATES: int = 3  # generic org patterns to try per record

# --- Zuhal circuit breaker ---
ZUHAL_CIRCUIT_BREAKER_COOLDOWN_SECONDS: float = 600.0  # 10 minutes

# --- Fallback domain blocklist ---
# Known directory/aggregator domains that should never be used as a business domain.
# This list grows at runtime via ProducerWorker._fallback_blocklist when a domain
# is seen as first-organic fallback for 2+ different businesses in the same run.
FALLBACK_DOMAIN_BLOCKLIST: frozenset[str] = frozenset({
    "bbb.org", "yelp.com", "yellowpages.com", "manta.com",
    "bizapedia.com", "opencorporates.com", "corporationwiki.com",
    "dnb.com", "zoominfo.com", "linkedin.com", "facebook.com",
    "instagram.com", "twitter.com", "x.com", "google.com",
    "mapquest.com", "whitepages.com", "spokeo.com", "intelius.com",
    "bizstanding.com", "bizjournals.com", "inc.com", "bloomberg.com",
})

# --- Consumer polling ---
CONSUMER_POLL_MAX_INTERVAL_SECONDS: int = 30
CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD: int = 3  # doubles interval after N consecutive empties
