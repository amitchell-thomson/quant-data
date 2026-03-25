from .base_client import BaseClient

PROVIDERS = {}


def register(name: str):
    """Decorator to register a provider class with a given name."""

    def decorator(cls):
        PROVIDERS[name] = cls
        return cls

    return decorator


def get_client(provider_name: str, provider_config: dict) -> BaseClient:
    """Takes provider name string and the providers config dict, looks up the class in PROVIDERS and instantiates it"""
    if provider_name not in PROVIDERS:
        raise ValueError(f"Provider '{provider_name}' is not registered. Available providers: {list(PROVIDERS)}")
    return PROVIDERS[provider_name](provider_config)
