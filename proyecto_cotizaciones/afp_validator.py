import base64
import glob
import os
import re
import time
from typing import Optional

import requests

from .afp_rules import AFP_RULES

try:
    from pypasser import reCaptchaV3  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    reCaptchaV3 = None

try:
    from selenium.webdriver import Chrome  # type: ignore
    from selenium.webdriver.chrome.options import Options  # type: ignore
    from selenium.webdriver.chrome.service import Service  # type: ignore
    from selenium.webdriver.common.by import By  # type: ignore
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore
    from selenium.webdriver.support.wait import WebDriverWait  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Chrome = None
    Options = None
    Service = None
    By = None
    EC = None
    WebDriverWait = None


class AFPValidationError(Exception):
    """Domain error for AFP validation."""


class AFPValidator:
    def __init__(
        self,
        timeout_secs: int,
        sleep_secs: float,
        enable_selenium: bool,
        selenium_driver_path: str,
        selenium_download_dir: str,
    ) -> None:
        self.timeout_secs = timeout_secs
        self.sleep_secs = sleep_secs
        self.enable_selenium = enable_selenium
        self.selenium_driver_path = selenium_driver_path
        self.selenium_download_dir = selenium_download_dir
        self.session = requests.Session()
        self._driver = None
        os.makedirs(self.selenium_download_dir, exist_ok=True)

    def close(self) -> None:
        if self._driver is not None:
            self._driver.quit()
            self._driver = None

    def download_pdf(self, afp: str, rut: str, codver: str) -> bytes:
        if afp == "habitat":
            return self._download_habitat(codver=codver)
        if afp == "provida":
            return self._download_provida(rut=rut, codver=codver)
        if afp == "uno":
            return self._download_uno(rut=rut, codver=codver)
        if afp == "planvital":
            return self._download_planvital(rut=rut, codver=codver)
        if afp == "modelo":
            return self._download_modelo(rut=rut, codver=codver)
        if afp == "cuprum":
            return self._download_cuprum(rut=rut, codver=codver)
        if afp == "capital":
            return self._download_capital(codver=codver)
        raise AFPValidationError(f"AFP no soportada: {afp}")

    def _download_habitat(self, codver: str) -> bytes:
        config = AFP_RULES["habitat"]
        response = self.session.post(
            config["download_url"],
            data={"folio": codver},
            headers=config.get("headers", {}),
            timeout=self.timeout_secs,
        )
        if response.status_code != 200:
            raise AFPValidationError(f"Habitat HTTP {response.status_code}")

        payload = response.json()
        if payload.get("resultado", "") != 0:
            raise AFPValidationError("Habitat sin resultado válido")

        time.sleep(self.sleep_secs)
        file_response = self.session.get(
            config["domain"] + payload.get("mensaje", ""),
            timeout=self.timeout_secs,
        )
        if file_response.status_code != 200:
            raise AFPValidationError(f"Habitat descarga HTTP {file_response.status_code}")
        return file_response.content

    def _download_provida(self, rut: str, codver: str) -> bytes:
        config = AFP_RULES["provida"]
        reqid = re.sub(r"\.", "", codver) + "-" + re.sub(r"[\.-]", "", rut)
        response = self.session.get(
            config["download_url"],
            params={"Id": reqid},
            headers=config.get("headers", {}),
            timeout=self.timeout_secs,
        )
        if response.status_code != 200:
            raise AFPValidationError(f"Provida HTTP {response.status_code}")
        if not response.content:
            raise AFPValidationError("Provida respuesta vacía")
        return response.content

    def _download_uno(self, rut: str, codver: str) -> bytes:
        config = AFP_RULES["uno"]
        reqid = re.sub(r"[\.-]", "", rut)
        response = self.session.post(
            config["download_url"],
            json={"payload": {"idPersona": reqid, "FolioCertificado": codver}},
            headers=config.get("headers", {}),
            timeout=self.timeout_secs,
        )
        if response.status_code != 200:
            raise AFPValidationError(f"UNO HTTP {response.status_code}")

        payload = response.json()
        if payload.get("codigo", "") != "0":
            raise AFPValidationError("UNO sin resultado válido")
        return base64.b64decode(payload.get("data", {}).get("bytes", "").encode())

    def _download_planvital(self, rut: str, codver: str) -> bytes:
        if reCaptchaV3 is None:
            raise AFPValidationError("pypasser no disponible para PlanVital")

        config = AFP_RULES["planvital"]
        captcha_token = reCaptchaV3(config["captcha_url"])
        params = {
            "certificateId": re.sub(r"[-]", "", codver),
            "rut": re.sub(r"[-\.]", "", rut),
        }
        headers = {**config.get("headers", {}), "Recaptcha-Token": captcha_token}

        self.session.get(
            config["download_url"],
            params=params,
            headers=headers,
            timeout=self.timeout_secs,
            verify=False,
        )
        time.sleep(self.sleep_secs)
        response = self.session.get(
            config["download_url"],
            params=params,
            headers=headers,
            timeout=self.timeout_secs,
            verify=False,
        )
        if response.status_code != 200:
            raise AFPValidationError(f"PlanVital HTTP {response.status_code}")

        payload = response.json()
        if not payload.get("valid", False):
            raise AFPValidationError("PlanVital sin validación")
        return base64.b64decode(payload.get("data", "").encode())

    def _download_modelo(self, rut: str, codver: str) -> bytes:
        driver = self._ensure_driver()
        config = AFP_RULES["modelo"]
        driver.get(config["browser_url"])
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[1]/div[2]/input').send_keys(rut)
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[2]/div[2]/input').send_keys(codver)
        button = driver.find_element(By.XPATH, '//*[@id="B-000020"]')
        driver.execute_script("arguments[0].click();", button)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a'))
        )
        url = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a').get_attribute("href")
        response = self.session.get(url, timeout=self.timeout_secs)
        if response.status_code != 200:
            raise AFPValidationError(f"Modelo descarga HTTP {response.status_code}")
        return response.content

    def _download_cuprum(self, rut: str, codver: str) -> bytes:
        driver = self._ensure_driver()
        config = AFP_RULES["cuprum"]
        driver.get(config["browser_url"])
        driver.find_element(By.ID, "txtRUT").send_keys(rut)
        driver.find_element(By.ID, "intFolio").send_keys(re.sub("CU", "", codver))
        driver.find_element(By.ID, "btnaceptar").click()

        session = requests.Session()
        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"])
        response = session.get(config["download_url"], timeout=self.timeout_secs)
        if response.status_code != 200:
            raise AFPValidationError(f"Cuprum descarga HTTP {response.status_code}")
        return response.content

    def _download_capital(self, codver: str) -> bytes:
        driver = self._ensure_driver()
        config = AFP_RULES["capital"]

        regex_parts = {
            "p1": re.match(r"([A-Z0-9]{5})-", codver),
            "p2": re.match(r"([A-Z0-9]{5}-){1}([A-Z0-9]{5})", codver),
            "p3": re.match(r"([A-Z0-9]{5}-){2}([A-Z0-9]{5})", codver),
            "p4": re.match(r"([A-Z0-9]{5}-){3}([A-Z0-9]{5})", codver),
            "dv": re.match(r"([A-Z0-9]{5}-){4}([0-9]{1})", codver),
        }
        if not all(regex_parts.values()):
            raise AFPValidationError(f"Formato codver inválido para Capital: {codver}")

        before_files = set(glob.glob(os.path.join(self.selenium_download_dir, "*")))
        driver.get(config["browser_url"])
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado1").send_keys(regex_parts["p1"].group(1))
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado2").send_keys(regex_parts["p2"].group(2))
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado3").send_keys(regex_parts["p3"].group(2))
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado4").send_keys(regex_parts["p4"].group(2))
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtDigito").send_keys(regex_parts["dv"].group(2))

        validate_button = driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnValida")
        driver.execute_script("arguments[0].click();", validate_button)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf"))
        )
        download_button = driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf")
        driver.execute_script("arguments[0].click();", download_button)

        downloaded_file = self._wait_for_download(before_files=before_files)
        with open(downloaded_file, "rb") as handle:
            content = handle.read()
        os.remove(downloaded_file)
        return content

    def _wait_for_download(self, before_files: set, timeout: int = 30) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            now_files = set(glob.glob(os.path.join(self.selenium_download_dir, "*")))
            new_files = [
                file_path
                for file_path in (now_files - before_files)
                if not file_path.endswith(".crdownload")
            ]
            if new_files:
                return max(new_files, key=os.path.getctime)
            time.sleep(0.5)
        raise AFPValidationError("Capital: timeout esperando PDF")

    def _ensure_driver(self):
        if not self.enable_selenium:
            raise AFPValidationError("Selenium deshabilitado en la configuración")
        if self._driver is not None:
            return self._driver
        if not all([Chrome, Options, Service, By, EC, WebDriverWait]):
            raise AFPValidationError("Dependencias Selenium no disponibles")
        if not os.path.exists(self.selenium_driver_path):
            raise AFPValidationError(f"Chromedriver no encontrado: {self.selenium_driver_path}")

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": self.selenium_download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
            },
        )

        self._driver = Chrome(service=Service(self.selenium_driver_path), options=options)
        self._driver.set_page_load_timeout(self.timeout_secs)
        return self._driver
