#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Authentication module for real estate crawler
"""

import time
import logging
import random
from datetime import datetime
from typing import Dict, Tuple, Optional

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from src.config import Config


class AuthenticationError(Exception):
    """Exception raised for authentication failures"""
    pass


class SessionExpiredError(AuthenticationError):
    """Exception raised when session has expired"""
    pass


class AuthSelectors:
    """CSS/XPath selectors for authentication"""
    LOGIN_BUTTON_TEXT_XPATH = "//button[contains(text(), '로그인') or contains(., '로그인')]"
    LOGIN_LINK_TEXT_XPATH = "//a[contains(text(), '로그인') or contains(., '로그인')]"
    LOGIN_BUTTON_CSS = (
        "button.inline-flex.items-center.justify-center.whitespace-nowrap.rounded-md"
        ".transition-colors.focus-visible\\:outline-none.focus-visible\\:ring-1"
        ".focus-visible\\:ring-ring.disabled\\:pointer-events-none.disabled\\:opacity-50"
        ".hover\\:bg-accent.hover\\:text-accent-foreground.h-10\\.5.w-10\\.5.cursor-pointer"
        ".border-none.bg-transparent.bg-none.bg-auto.p-1.px-0.font-semibold"
        ".text-\\[\\#222222\\].no-underline.text-sm"
    )
    EMAIL_INPUT = "input[placeholder='이메일 또는 아이디']"
    PASSWORD_INPUT = "input[type='password']"
    SUBMIT_BUTTON_XPATH = "//form//button[contains(., '로그인')]"
    SUBMIT_BUTTON_FALLBACK_XPATH = "//form/div/div[contains(@class, 'flex')]/button"


class AuthIndicators:
    """Strings indicating login status"""
    LOGOUT = "로그아웃"
    MY_PAGE = "마이페이지"
    PROFILE = "프로필"
    URL_MYPAGE = "mypage"
    URL_DASHBOARD = "dashboard"


class Authenticator:
    """Handles login and authentication for the crawler"""
    
    def __init__(self, config: Optional[Config] = None):
        """Initialize authenticator with configuration"""
        self.config = config or Config.get_instance()
        self.session = requests.Session()
        self.driver: Optional[webdriver.Chrome] = None
        
        # Authentication state
        self.auth_headers: Dict[str, str] = {"User-Agent": self.config.user_agent}
        self.last_auth_time: Optional[datetime] = None
        self.max_retries = 3
        self.session_timeout = 1800  # 30 minutes in seconds
        self.logger = logging.getLogger(__name__)
    
    def login(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Handle login process using browser automation
        
        Returns:
            Tuple of (auth_headers, webdriver) for subsequent requests
            
        Raises:
            AuthenticationError: If all login attempts fail
        """
        self.logger.info("Starting login process")
        
        for attempt in range(1, self.max_retries + 1):
            try:
                # Browser login
                headers, driver = self._browser_login()
                if headers:
                    self.auth_headers = headers
                    self.last_auth_time = datetime.now()
                    return self.auth_headers, driver
                    
            except (AuthenticationError, WebDriverException) as e:
                self.logger.error(f"Login attempt {attempt} failed: {e}")
                if attempt == self.max_retries:
                    raise AuthenticationError(f"All login attempts failed: {e}")
                
                # Exponential backoff with jitter
                backoff_time = (2 ** attempt) + random.uniform(0, 1)
                self.logger.info(f"Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
        
        raise AuthenticationError("Login failed after multiple attempts")
    
    def ensure_authenticated(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Ensure the session is authenticated, re-login if necessary
        
        Returns:
            Tuple of (auth_headers, webdriver) for subsequent requests
        """
        if self._needs_reauth():
            self.logger.info("Session expired or not authenticated, re-authenticating...")
            return self.login()
        
        self.logger.debug("Session is still valid")
        return self.auth_headers, self.driver
    
    def _browser_login(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Attempt login via browser automation
        
        Returns:
            Tuple of (auth_headers, webdriver)
        
        Raises:
            AuthenticationError: If login fails
        """
        self.logger.info("Attempting browser login")
        
        self._ensure_driver()
        
        try:
            self._navigate_to_login_page()
            self._perform_login()
            self._verify_login_success()
            
            # Navigate to community list page after successful login
            self.driver.get(self.config.specific_list_url)
            time.sleep(self.config.wait_page_load)
            
            return self._extract_session_headers(), self.driver
            
        except Exception as e:
            self.logger.error(f"Browser login failed: {e}")
            raise AuthenticationError(f"Browser login failed: {e}")

    def _ensure_driver(self) -> None:
        """Initialize webdriver if needed"""
        if self.driver is None:
            try:
                self.driver = self._create_driver()
            except Exception as e:
                raise AuthenticationError(f"Failed to create webdriver: {e}")

    def _navigate_to_login_page(self) -> None:
        """Navigate to the site and open the login modal/page"""
        self.driver.get(self.config.specific_list_url)
        time.sleep(self.config.wait_page_load)
        
        # Click login button
        login_button = self._find_login_button()
        self.driver.execute_script("arguments[0].click();", login_button)
        time.sleep(self.config.wait_page_load)

    def _find_login_button(self):
        """Find the initial login button using multiple strategies"""
        try:
            return self.driver.find_element(By.XPATH, AuthSelectors.LOGIN_BUTTON_TEXT_XPATH)
        except NoSuchElementException:
            try:
                return self.driver.find_element(By.XPATH, AuthSelectors.LOGIN_LINK_TEXT_XPATH)
            except NoSuchElementException:
                return self.driver.find_element(By.CSS_SELECTOR, AuthSelectors.LOGIN_BUTTON_CSS)

    def _perform_login(self) -> None:
        """Fill and submit the login form"""
        # Fill email
        email_input = self.driver.find_element(By.CSS_SELECTOR, AuthSelectors.EMAIL_INPUT)
        email_input.clear()
        email_input.send_keys(self.config.login_id)
        time.sleep(1)
        
        # Fill password
        password_input = self.driver.find_element(By.CSS_SELECTOR, AuthSelectors.PASSWORD_INPUT)
        password_input.clear()
        password_input.send_keys(self.config.login_pw)
        time.sleep(1)
        
        # Submit
        submit_button = self._find_submit_button()
        self.driver.execute_script("arguments[0].click();", submit_button)
        time.sleep(self.config.wait_after_login)

    def _find_submit_button(self):
        """Find the submit button"""
        try:
            return self.driver.find_element(By.XPATH, AuthSelectors.SUBMIT_BUTTON_XPATH)
        except NoSuchElementException:
            return self.driver.find_element(By.XPATH, AuthSelectors.SUBMIT_BUTTON_FALLBACK_XPATH)

    def _verify_login_success(self) -> None:
        """Verify that login was successful"""
        if not self._is_logged_in_browser():
            raise AuthenticationError("Browser login verification failed")
        self.logger.info("Browser login successful")

    def _extract_session_headers(self) -> Dict[str, str]:
        """Extract cookies and create session headers"""
        cookies = self.driver.get_cookies()
        headers = {"User-Agent": self.config.user_agent}
        
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'])
            
        return headers

    def _is_logged_in_browser(self) -> bool:
        """
        Check if browser login was successful
        
        Returns:
            True if logged in, False otherwise
        """
        try:
            # Check for login indicators in the page
            page_source = self.driver.page_source.lower()
            
            # Check for common login success indicators
            success_indicators = [
                AuthIndicators.LOGOUT,
                AuthIndicators.MY_PAGE,
                AuthIndicators.PROFILE,
                self.config.login_id.lower()
            ]
            
            for indicator in success_indicators:
                if indicator in page_source:
                    return True
            
            # Check URL for success indicators
            current_url = self.driver.current_url.lower()
            if AuthIndicators.URL_MYPAGE in current_url or AuthIndicators.URL_DASHBOARD in current_url:
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking login status: {e}")
            return False
    
    def _needs_reauth(self) -> bool:
        """
        Check if session needs re-authentication
        
        Returns:
            True if re-authentication is needed, False otherwise
        """
        # Not authenticated yet
        if not self.last_auth_time:
            return True
        
        # Check session age
        session_age = datetime.now() - self.last_auth_time
        if session_age.total_seconds() > self.session_timeout:
            self.logger.info(f"Session expired after {session_age.total_seconds():.0f} seconds")
            return True
        
        return False
    
    def _create_driver(self) -> webdriver.Chrome:
        """
        Create and configure Chrome webdriver
        
        Returns:
            Configured Chrome webdriver
        """
        options = Options()
        options.headless = self.config.browser_options["headless"]
        # Enable performance logging for Network events
        perf_prefs = {"performance": "ALL"}
        options.set_capability('goog:loggingPrefs', perf_prefs)
        
        if self.config.browser_options["disable_automation"]:
            options.add_argument("--disable-blink-features=AutomationControlled")
        
        if self.config.browser_options["no_sandbox"]:
            options.add_argument("--no-sandbox")
            
        if self.config.browser_options["disable_shm"]:
            options.add_argument("--disable-dev-shm-usage")
            
        options.add_argument(f'user-agent={self.config.user_agent}')
        
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
    
    def close(self):
        """Close browser and clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error closing webdriver: {e}")
            finally:
                self.driver = None
        
        # Clear session
        self.session.close()
