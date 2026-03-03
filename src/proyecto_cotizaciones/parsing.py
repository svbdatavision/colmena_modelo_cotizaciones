import re
from typing import Optional

from .afp_rules import AFP_RULES


def normalize_afp_from_text(text: str) -> str:
    afp_match = re.search(r"\n+A\.?F\.?P\.? +(\w+) ?(S\.A\.)?", text, re.IGNORECASE)
    if afp_match:
        return afp_match.group(1).lower()
    if re.search(r"afphabitat\.cl", text, re.IGNORECASE):
        return "habitat"
    return ""


def extract_rut(text: str) -> str:
    rut_match = re.search(r"\d{1,2}(?:\.\d{3}){2}-[\dkK]", text)
    return rut_match.group(0) if rut_match else ""


def extract_codver(afp: str, text: str) -> str:
    if not afp or afp not in AFP_RULES:
        return ""
    codver_regex = AFP_RULES[afp]["codver_regex"]
    codver_match = re.search(codver_regex, text, re.IGNORECASE)
    return codver_match.group(1) if codver_match else ""


def is_certificado_cotizaciones(text: str) -> bool:
    return bool(re.search(r"certificado(?:\s+\w+){,2}\s+cotizaciones", text, re.IGNORECASE))


def metadata_matches(afp: str, metadata_producer: str) -> Optional[bool]:
    if not afp or afp not in AFP_RULES:
        return None
    producer_pattern = AFP_RULES[afp]["metadata"]["producer"]
    if not producer_pattern:
        return None
    return bool(re.search(producer_pattern, metadata_producer or "", re.IGNORECASE))


def calculate_rut_l11(rut: str) -> str:
    if not rut:
        return ""
    return rut.replace(".", "").zfill(11)
