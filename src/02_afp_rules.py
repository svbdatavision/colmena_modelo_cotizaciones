AFP_RULES = {
    "modelo": {
        "display_name": "A.F.P. Modelo S.A.",
        "codver_regex": r"Folio.*:\s+([a-z\d]+)",
        "metadata": {
            "creator": "Crystal Reports",
            "producer": "Powered By Crystal",
        },
        "download_url": "https://api-kong-preprod.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados",
        "browser_url": "https://nueva.afpmodelo.cl/empleadores/herramientas-empleadores/validar-certificados",
    },
    "habitat": {
        "display_name": "AFP Habitat",
        "codver_regex": r"([a-z\d]{8}-(?:[a-z\d]{4}-){3}[a-z\d]{12})",
        "metadata": {
            "creator": "JasperReports",
            "producer": "iText1.3.1",
        },
        "download_url": "https://www.afphabitat.cl/wp-admin/admin-ajax.php?action=ajax_call&funcion=getValidaCertificado",
        "domain": "https://www.afphabitat.cl",
        "headers": {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "es-ES,es;q=0.5",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
        },
    },
    "cuprum": {
        "display_name": "AFP CUPRUM S.A.",
        "codver_regex": r"FOLIO\s+N.?\s+CU(\d+)",
        "metadata": {
            "creator": "",
            "producer": "null",
        },
        "browser_url": "https://www.cuprum.cl/wwwPublico/ValidaCertificados/Inicio.aspx",
        "download_url": "https://www.cuprum.cl/wwwPublico/ValidaCertificados/Validar.aspx?ID=",
    },
    "capital": {
        "display_name": "AFP CAPITAL S.A.",
        "codver_regex": r"certificacion:\s+([a-z|\d|-]+)",
        "metadata": {
            "creator": "PDFsharp",
            "producer": "PDFsharp",
        },
        "browser_url": "https://www.afpcapital.cl/Empleador/Paginas/Validador-de-Certificados.aspx?IDList=10",
    },
    "provida": {
        "display_name": "AFP ProVida S.A.",
        "codver_regex": r"certificado:\s+([\d|\.]+)",
        "metadata": {
            "creator": "",
            "producer": "iText",
        },
        "download_url": "https://w3.provida.cl/validador/descarga.ashx",
        "headers": {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.9,en;q=0.8,es-CL;q=0.7",
        },
    },
    "planvital": {
        "display_name": "AFP PlanVital S.A.",
        "codver_regex": r"Folio\s+([\d-]+)",
        "metadata": {
            "creator": "Telerik Reporting",
            "producer": "Telerik Reporting",
        },
        "download_url": "https://api2.planvital.cl/public/certificates/validate-certificate",
        "captcha_url": "https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LdLsLcZAAAAABa5_AM2INGgCz6uszjY6EkzTBMT&co=aHR0cHM6Ly93d3cucGxhbnZpdGFsLmNsOjQ0Mw..&hl=es&v=rz4DvU-cY2JYCwHSTck0_qm-&size=invisible&cb=hx6152fusotd",
        "headers": {
            "accept": "application/json, text/plain, */*",
            "accept-language": "es-ES,es;q=0.9",
            "referer": "https://www.planvital.cl/",
        },
    },
    "uno": {
        "display_name": "AFP UNO",
        "codver_regex": r"Certificacion\s+N.?:\s+([a-z|\d]+)",
        "metadata": {
            "creator": "Crystal Reports",
            "producer": "Powered By Crystal",
        },
        "download_url": "https://www.uno.cl/api/afiliado-certificado/validar",
        "headers": {
            "accept": "application/json, text/plain, */*",
            "accept-language": "es-ES,es;q=0.8",
            "content-type": "application/json;charset=UTF-8",
        },
    },
}
