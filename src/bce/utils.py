"""Normalisation numéros BCE / TVA."""

import re


def normalize_bce(num: str) -> str:
    return re.sub(r"\D", "", str(num)).zfill(10)


def format_bce(num: str) -> str:
    n = normalize_bce(num)
    return f"{n[:4]}.{n[4:7]}.{n[7:]}"


def tva_from_bce(num: str) -> str:
    return f"BE{normalize_bce(num)}"
