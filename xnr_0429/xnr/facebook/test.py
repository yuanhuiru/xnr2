import time

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

open_driver_count = 0
options = Options()
options.add_argument('-headless')
# driver = webdriver.PhantomJS()
driver = webdriver.Firefox(firefox_options=options)
driver.get('https://www.facebook.com')
wait = WebDriverWait(driver, timeout=30)
time.sleep(5)
html = driver.page_source

with open('test.html', 'wb') as f:
    f.write(html)
driver.find_element_by_xpath('//input[@id="email"]').send_keys("+8613269704912")
driver.find_element_by_xpath('//input[@id="pass"]').send_keys("chenhuiping")
driver.find_element_by_xpath('//input[@data-testid="royal_login_button"]').click()
driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
# driver.save_screenshot('launcher.png')
# driver.find_element_by_xpath('//div[@data-click="home_icon"]/a').click()
# driver.save_screenshot("keep.png")
# driver.find_element_by_xpath('//*[@id="rc.u_0_13"]/div[1]/span[1]/a/span/span[1]').click()
# """//*[@id="rc.u_0_13"]/div[1]/span[1]/a/span/span[1]"""
driver.find_element_by_xpath('//div[@class="_1k67 _cy7"]').click()
html = driver.page_source
with open('test.html', 'wb') as f:
    f.write(html)
print 1
# time.sleep(10)
textbox = wait.until(
    EC.presence_of_element_located((By.XPATH, '//div[@class="_3nd0"]'))
)
textbox.click()
# driver.find_element_by_xpath('//div[@class="_3nd0"]').click()

# driver.find_element_by_xpath('//div[@id="placeholder-ahd13"]').click()
time.sleep(5)
driver.find_element_by_xpath('//div[@aria-autocomplete="list"]').send_keys("hhhhhhhh")
driver.find_element_by_xpath('//button[@data-testid="react-composer-post-button"]').click()

driver.close()
