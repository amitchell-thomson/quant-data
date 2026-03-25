from abc import ABC, abstractmethod

import pandas as pd


class BaseClient(ABC):
    @abstractmethod
    def fetch(self, dataset_cfg: dict, year: int | None = None) -> pd.DataFrame: ...

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
