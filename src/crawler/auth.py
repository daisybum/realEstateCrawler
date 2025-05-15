#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Authentication module for real estate crawler
"""

import time
import logging
import random
import re
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, Any, Union

import requests
import cloudscraper
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from src.config import Config


class AuthenticationError(Exception):
    """Exception raised for authentication failures"""
    pass


class SessionExpiredError(AuthenticationError):
    """Exception raised when session has expired"""
    pass


class CSRFTokenError(AuthenticationError):
    """Exception raised when CSRF token cannot be extracted"""
    pass


class Authenticator:
    """Handles login and authentication for the crawler"""
    
    def __init__(self, config=None):
        """Initialize authenticator with configuration"""
        self.config = config or Config.get_instance()
        self.session = requests.Session()
        self.scraper = cloudscraper.create_scraper()
        self.driver = None
        
        # Authentication state
        self.csrf_token = None
        self.auth_headers = {"User-Agent": self.config.user_agent}
        self.last_auth_time = None
        self.auth_method = None  # 'api' or 'browser'
        self.max_retries = 3
        self.session_timeout = 1800  # 30 minutes in seconds
    
    def login(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Handle login process, first with API, then fallback to browser
        
        Returns:
            Tuple of (auth_headers, webdriver) for subsequent requests
            
        Raises:
            AuthenticationError: If all login attempts fail
        """
        logging.info("Starting login process")
        
        # Try API login first
        for attempt in range(1, self.max_retries + 1):
            try:
                # Try API login
                auth_headers = self._api_login()
                if auth_headers:
                    self.auth_headers = auth_headers
                    self.last_auth_time = datetime.now()
                    self.auth_method = 'api'
                    return self.auth_headers, None
                
                # Fall back to browser login if API fails
                headers, driver = self._browser_login()
                if headers:
                    self.auth_headers = headers
                    self.last_auth_time = datetime.now()
                    self.auth_method = 'browser'
                    return self.auth_headers, driver
                    
            except (AuthenticationError, WebDriverException) as e:
                logging.error(f"Login attempt {attempt} failed: {e}")
                if attempt == self.max_retries:
                    raise AuthenticationError(f"All login attempts failed: {e}")
                
                # Exponential backoff with jitter
                backoff_time = (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
        
        raise AuthenticationError("Login failed after multiple attempts")
    
    def ensure_authenticated(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Ensure the session is authenticated, re-login if necessary
        
        Returns:
            Tuple of (auth_headers, webdriver) for subsequent requests
        """
        if self._needs_reauth():
            logging.info("Session expired or not authenticated, re-authenticating...")
            return self.login()
        
        logging.debug("Session is still valid")
        return self.auth_headers, self.driver
    
    def _api_login(self) -> Optional[Dict[str, str]]:
        """
        Attempt login via API
        
        Returns:
            Authentication headers if successful, None otherwise
            
        Raises:
            AuthenticationError: If login fails due to invalid credentials
        """
        logging.info("Attempting API login")
        
        # Get CSRF token if needed
        if not self.csrf_token:
            try:
                self._extract_csrf_token_from_api()
            except CSRFTokenError as e:
                logging.warning(f"Could not extract CSRF token: {e}")
                # Continue without CSRF token, some APIs don't require it
        
        # Prepare login data
        login_data = {
            "email": self.config.login_id,
            "password": self.config.login_pw
        }
        
        # Add CSRF token if available
        if self.csrf_token:
            login_data["csrf_token"] = self.csrf_token
        
        # Set up headers
        headers = {
            "User-Agent": self.config.user_agent,
            "Content-Type": "application/json",
            "Referer": self.config.base_url
        }
        
        try:
            # Attempt login
            r = self.scraper.post(
                self.config.login_url, 
                json=login_data,
                headers=headers,
                timeout=self.config.request_timeout
            )
            
            # Check for successful login
            if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
                resp = r.json()
                if "accessToken" in resp:
                    token = resp["accessToken"]
                    logging.info("API login successful")
                    
                    # Update session cookies
                    self.session.cookies.update(self.scraper.cookies)
                    
                    return {
                        "User-Agent": self.config.user_agent,
                        "Authorization": f"Bearer {token}"
                    }
            
            # Check for specific error messages
            if r.status_code == 401 or r.status_code == 403:
                error_msg = "Invalid credentials" if r.status_code == 401 else "Access forbidden"
                if r.headers.get("content-type", "").startswith("application/json"):
                    try:
                        error_data = r.json()
                        error_msg = error_data.get("message", error_msg)
                    except Exception:
                        pass
                raise AuthenticationError(f"API login failed: {error_msg}")
            
            logging.warning(f"API login failed with status code {r.status_code}")
            return None
            
        except requests.RequestException as e:
            logging.error(f"API login request failed: {e}")
            return None
    
    def _extract_csrf_token_from_api(self) -> Optional[str]:
        """
        Extract CSRF token from login page via API
        
        Returns:
            CSRF token if found, None otherwise
            
        Raises:
            CSRFTokenError: If CSRF token cannot be extracted
        """
        try:
            r = self.scraper.get(
                self.config.login_url,
                headers={"User-Agent": self.config.user_agent},
                timeout=self.config.request_timeout
            )
            
            if r.status_code == 200:
                # Try to find CSRF token in HTML
                soup = BeautifulSoup(r.text, 'html.parser')
                
                # Look for common CSRF token patterns
                csrf_selectors = [
                    "input[name='csrf_token']", 
                    "input[name='_csrf_token']",
                    "input[name='_token']",
                    "meta[name='csrf-token']"
                ]
                
                for selector in csrf_selectors:
                    token_elem = soup.select_one(selector)
                    if token_elem:
                        if token_elem.name == 'input':
                            self.csrf_token = token_elem.get('value')
                        elif token_elem.name == 'meta':
                            self.csrf_token = token_elem.get('content')
                        
                        if self.csrf_token:
                            logging.info(f"CSRF token extracted: {self.csrf_token[:10]}...")
                            return self.csrf_token
                
                # Try to find token in JavaScript
                js_pattern = re.compile(r'csrfToken["\s]*[:=]["\s]*["\'](.*?)["\']', re.IGNORECASE)
                match = js_pattern.search(r.text)
                if match:
                    self.csrf_token = match.group(1)
                    logging.info(f"CSRF token extracted from JS: {self.csrf_token[:10]}...")
                    return self.csrf_token
                
                logging.warning("Could not find CSRF token in login page")
            else:
                logging.warning(f"Failed to get login page, status code: {r.status_code}")
            
            return None
            
        except requests.RequestException as e:
            raise CSRFTokenError(f"Failed to get login page: {e}")
    
    def _browser_login(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
        """
        Attempt login via browser automation
        
        Returns:
            Tuple of (auth_headers, webdriver)
        
        Raises:
            AuthenticationError: If login fails
        """
        logging.info("Attempting browser login")
        
        # Initialize webdriver if needed
        if self.driver is None:
            try:
                self.driver = self._create_driver()
            except Exception as e:
                raise AuthenticationError(f"Failed to create webdriver: {e}")
        
        try:
            # Navigate to community page
            self.driver.get(self.config.specific_list_url)
            time.sleep(self.config.wait_page_load)
            
            # Click login button
            login_button = self.driver.find_element(By.CSS_SELECTOR, 
                "button.inline-flex.items-center.justify-center.whitespace-nowrap.rounded-md.transition-colors.focus-visible\\:outline-none.focus-visible\\:ring-1.focus-visible\\:ring-ring.disabled\\:pointer-events-none.disabled\\:opacity-50.hover\\:bg-accent.hover\\:text-accent-foreground.h-10\\.5.w-10\\.5.cursor-pointer.border-none.bg-transparent.bg-none.bg-auto.p-1.px-0.font-semibold.text-\\[\\#222222\\].no-underline.text-sm")
            self.driver.execute_script("arguments[0].click();", login_button)
            time.sleep(self.config.wait_page_load)
            
            # Fill login form
            email_input = self.driver.find_element(By.CSS_SELECTOR, "input[placeholder='이메일 또는 아이디']")
            email_input.clear()
            email_input.send_keys(self.config.login_id)
            time.sleep(1)
            
            password_input = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
            password_input.clear()
            password_input.send_keys(self.config.login_pw)
            time.sleep(1)
            
            # Submit login
            submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            self.driver.execute_script("arguments[0].click();", submit_button)
            time.sleep(self.config.wait_after_login)
            
            # Verify login success
            if not self._is_logged_in_browser():
                raise AuthenticationError("Browser login verification failed")
            
            # Extract cookies and apply to session
            cookies = self.driver.get_cookies()
            headers = {"User-Agent": self.config.user_agent}
            
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
                self.scraper.cookies.set(cookie['name'], cookie['value'])
                
            logging.info("Browser login successful")
            return headers, self.driver
            
        except Exception as e:
            logging.error(f"Browser login failed: {e}")
            raise AuthenticationError(f"Browser login failed: {e}")
    
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
                "로그아웃",  # "logout" in Korean
                "마이페이지",  # "my page" in Korean
                "프로필",  # "profile" in Korean
                self.config.login_id.lower()  # Username visible
            ]
            
            for indicator in success_indicators:
                if indicator in page_source:
                    return True
            
            # Check URL for success indicators
            current_url = self.driver.current_url.lower()
            if "mypage" in current_url or "dashboard" in current_url:
                return True
                
            return False
            
        except Exception as e:
            logging.error(f"Error checking login status: {e}")
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
            logging.info(f"Session expired after {session_age.total_seconds():.0f} seconds")
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
                logging.error(f"Error closing webdriver: {e}")
            finally:
                self.driver = None
        
        # Clear session
        self.session.close()
        self.scraper.close()
