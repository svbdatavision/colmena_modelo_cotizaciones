from io import BytesIO
import unicodedata

from pdfminer.high_level import extract_text
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser


def _decode_meta_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode()
        except Exception:
            return ""
    return str(value)


def extract_pdf_metadata(pdf_bytes: bytes) -> dict:
    if not pdf_bytes:
        return {
            "metadata_creator": "",
            "metadata_producer": "",
            "metadata_creadate": "",
            "metadata_moddate": "",
        }

    parser = PDFParser(BytesIO(pdf_bytes))
    document = PDFDocument(parser)
    meta = document.info[0] if document.info else {}
    return {
        "metadata_creator": _decode_meta_value(meta.get("Creator", "")),
        "metadata_producer": _decode_meta_value(meta.get("Producer", "")),
        "metadata_creadate": _decode_meta_value(meta.get("CreationDate", "")),
        "metadata_moddate": _decode_meta_value(meta.get("ModDate", "")),
    }


def extract_pdf_text_normalized(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    text = extract_text(BytesIO(pdf_bytes))
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
