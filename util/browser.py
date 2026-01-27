from playwright.sync_api import sync_playwright

class Browser_Manager():
    def __init__(self) -> None:
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )  # headless=True 可无界面运行
