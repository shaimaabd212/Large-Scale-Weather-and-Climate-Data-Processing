"""Configuration loading and the path layout shared by every stage."""
import json
from dataclasses import dataclass
from pathlib import Path


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    return {k: v for k, v in cfg.items() if not k.startswith("_")}


@dataclass(frozen=True)
class Paths:
    base: str  # e.g. hdfs://bd-master:9000/weather  or  file:///home/me/repo/data

    def _p(self, *parts: str) -> str:
        return "/".join([self.base.rstrip("/"), *parts])

    # raw
    @property
    def raw_by_year(self): return self._p("raw", "by_year")
    @property
    def raw_meta(self): return self._p("raw", "meta")
    # bronze
    @property
    def bronze_obs(self): return self._p("bronze", "observations")
    @property
    def bronze_stations(self): return self._p("bronze", "stations")
    # silver
    @property
    def silver_daily(self): return self._p("silver", "daily")
    # quality
    def quality(self, name): return self._p("quality", name)
    # gold
    def gold(self, name): return self._p("gold", name)
    # export
    def export(self, name): return self._p("export", name)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]
