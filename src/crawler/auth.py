#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Authentication utilities for real estate crawler
"""

import time
import logging
from typing import Dict, Tuple, Optional

import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from src.config import Config


class Authenticator:
    """Handles login and authentication for the crawler"""
    
    def __init__(self):
        """Initialize authenticator with default settings"""
        self.scraper = cloudscraper.create_scraper()
        self.driver = None
        self.headers = {"User-Agent": Config.USER_AGENT}
    
    def login(self) -> Tuple[Dict[str, str], webdriver.Chrome]:
        """
        Handle login process, first with API, then fallback to browser
        
        Returns:
            Tuple of (auth_headers, webdriver) for subsequent requests
        """
        logging.info("로그인 시도 중...")
        
        # Try API login first
        try:
            auth_headers = self._api_login()
            if auth_headers:
                return auth_headers, self.driver
        except Exception as e:
            logging.error(f"API 로그인 실패: {e}")
        
        # Fall back to browser login
        return self._browser_login()
    
    def _api_login(self) -> Optional[Dict[str, str]]:
        """
        Attempt login via API
        
        Returns:
            Authentication headers if successful, None otherwise
        """
        login_data = {
            "email": Config.LOGIN_ID,
            "password": Config.LOGIN_PW
        }
        
        r = self.scraper.post(
            Config.LOGIN_URL, 
            json=login_data,
            headers={"User-Agent": Config.USER_AGENT},
            timeout=Config.REQUEST_TIMEOUT
        )
        
        if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
            resp = r.json()
            if "accessToken" in resp:
                token = resp["accessToken"]
                logging.info("API 로그인 성공")
                return {"User-Agent": Config.USER_AGENT, "Authorization": f"Bearer {token}"}
        
        return None
    
    def _browser_login(self) -> Tuple[Dict[str, str], webdriver.Chrome]:
        """
        Attempt login via browser automation
        
        Returns:
            Tuple of (auth_headers, webdriver)
        
        Raises:
            RuntimeError: If login fails
        """
        logging.info("브라우저 로그인 시도 중...")
        
        # Initialize webdriver if needed
        if self.driver is None:
            self.driver = self._create_driver()
        
        try:
            # Navigate to community page
            self.driver.get(Config.SPECIFIC_LIST_URL)
            time.sleep(Config.WAIT_PAGE_LOAD)
            
            # Click login button
            login_button = self.driver.find_element(By.CSS_SELECTOR, 
                "button.inline-flex.items-center.justify-center.whitespace-nowrap.rounded-md.transition-colors.focus-visible\\:outline-none.focus-visible\\:ring-1.focus-visible\\:ring-ring.disabled\\:pointer-events-none.disabled\\:opacity-50.hover\\:bg-accent.hover\\:text-accent-foreground.h-10\\.5.w-10\\.5.cursor-pointer.border-none.bg-transparent.bg-none.bg-auto.p-1.px-0.font-semibold.text-\\[\\#222222\\].no-underline.text-sm")
            self.driver.execute_script("arguments[0].click();", login_button)
            time.sleep(Config.WAIT_PAGE_LOAD)
            
            # Fill login form
            email_input = self.driver.find_element(By.CSS_SELECTOR, "input[placeholder='이메일 또는 아이디']")
            email_input.clear()
            email_input.send_keys(Config.LOGIN_ID)
            time.sleep(1)
            
            password_input = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
            password_input.clear()
            password_input.send_keys(Config.LOGIN_PW)
            time.sleep(1)
            
            # Submit login
            submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            self.driver.execute_script("arguments[0].click();", submit_button)
            time.sleep(Config.WAIT_AFTER_LOGIN)
            
            # Extract cookies and apply to scraper
            cookies = self.driver.get_cookies()
            headers = {"User-Agent": Config.USER_AGENT}
            
            for cookie in cookies:
                self.scraper.cookies.set(cookie['name'], cookie['value'])
                
            logging.info("브라우저 로그인 성공")
            return headers, self.driver
            
        except Exception as e:
            logging.error(f"브라우저 로그인 실패: {e}")
            raise RuntimeError("로그인 실패")
    
    def _create_driver(self) -> webdriver.Chrome:
        """
        Create and configure Chrome webdriver
        
        Returns:
            Configured Chrome webdriver
        """
        options = Options()
        options.headless = Config.BROWSER_OPTIONS["headless"]
        
        if Config.BROWSER_OPTIONS["disable_automation"]:
            options.add_argument("--disable-blink-features=AutomationControlled")
        
        if Config.BROWSER_OPTIONS["no_sandbox"]:
            options.add_argument("--no-sandbox")
            
        if Config.BROWSER_OPTIONS["disable_shm"]:
            options.add_argument("--disable-dev-shm-usage")
            
        options.add_argument(f'user-agent={Config.USER_AGENT}')
        
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
    
    def close(self):
        """Close browser if it's open"""
        if self.driver:
            self.driver.quit()
            self.driver = None
