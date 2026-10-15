# Databricks notebook source
import os
from typing import Generator, Any, List

import pandas as pd
import pyspark.sql.functions as F
import jellyfish as JF
from uuid6 import uuid7
from rapidfuzz.distance import JaroWinkler as JW
from rapidfuzz import fuzz as RF
from pyspark.sql.types import (
    StringType,
    FloatType,
    ArrayType
)

JARO_WINKLER_PENALTY_EXPONENTIAL = int(os.getenv(JW_PENALTY_EXP, '1'))
JARO_WINKLER_REWARD = float(os.getenv(JW_REWARD, '0.25'))

if JARO_WINKLER_REWARD < 0.0:
    JARO_WINKLER_REWARD = 0.0

if JARO_WINKLER_REWARD > 0.25:
    JARO_WINKLER_REWARD = 0.25

WEIGHT = float(os.getenv(RATIO_WEIGHT, '0.1'))
INV_WEIGHT = 1 - WEIGHT

RecordGen = Generator[List[Any], Any, None]

# COMMAND ----------

def partition(records: List[Any], size: int = 1_000) -> RecordGen:
    for index in range(0, len(records), size):
        yield records[index:index + size]

# COMMAND ----------

@F.udf(returnType=StringType())
def generate_uuid7_str() -> str:
    return str(uuid7()).upper()

@F.pandas_udf(ArrayType(StringType())) # type: ignore
def get_sound(series: pd.Series) -> pd.Series:
    def _get_sound(word: str) -> str:
        sound: str = JF.metaphone(word)

        if sound:
            return sound

        return word

    return series.apply(lambda arr: [_get_sound(str(c or '')) for c in arr] if
                        arr is not None else []) # type: ignore

@F.pandas_udf(returnType=FloatType()) # type: ignore
def match_terms(col1: pd.Series, col2: pd.Series) -> pd.Series:
    def match(term1, term2) -> float:
        if not term1 or not term2:
            return 0.0

        term1 = term1.lower()
        term2 = term2.lower()

        set_ratio = RF.token_set_ratio(term1, term2)
        sort_ratio = RF.token_sort_ratio(term1, term2)
        jaro_winkler = JW.similarity(
            term1.replace(' ',''),
            term2.replace(' ', ''),
            prefix_weight=JARO_WINKLER_REWARD
        )

        return (
            ((set_ratio * INV_WEIGHT) + (sort_ratio * WEIGHT)) *
            (jaro_winkler ** JARO_WINKLER_PENALTY_EXPONENTIAL)
        )

    return col1.combine(col2, lambda x, y: match(x, y))

