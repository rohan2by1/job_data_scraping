from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import pandas as pd
import os

# Configure WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in background
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=options)

# Function to safely extract text
def get_text_safe(driver, xpath):
    try:
        return driver.find_element(By.XPATH, xpath).text.strip()
    except NoSuchElementException:
        return "Not Available"

# Range of Job IDs to scrape
start_id = 3100
end_id = 4000  

# Base URLs
details_base_url = "https://thejobcompany.in/frontend/job_details?job_id="
apply_base_url = "https://thejobcompany.in/frontend/apply_page.php?job_id="

# List to store job data
jobs = []
wait = WebDriverWait(driver, 10)  # 10 seconds timeout

for job_id in range(start_id, end_id + 1):
    job_url = f"{details_base_url}{job_id}"
    apply_url = f"{apply_base_url}{job_id}"

    # Scrape Job Details
    driver.get(job_url)

    # Check if "No job found for this ID" exists
    if "No job found for this ID." in driver.page_source:
        print(f"❌ No job found for Job ID {job_id}. Stopping scraping.")
        break  # Stop the loop if no job is found

    job_name = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[1]")
    company = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[2]") 
    company_website = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[1]/div[1]/p[2]/a")
    location = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[2]/div[1]/p[2]")
    job_type = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[3]/div[1]/p[2]")
    batch = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[4]/div[1]/p[2]")
    stream_required = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[5]/div[1]/p[2]")
    salary = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[6]/div[1]/p[2]")
    updated_on = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[3]")[12:]  # Remove "Updated On:"

    # Scrape Apply Link
    driver.get(apply_url)
    try:
        apply_link = wait.until(EC.presence_of_element_located((By.ID, "applyButton"))).get_attribute("href")
    except (NoSuchElementException, TimeoutException):
        apply_link = "Apply Link Not Available"

    # Append data
    jobs.append([
        job_id, job_url, apply_link, job_name, company,
        company_website, location, job_type, batch, stream_required, salary, updated_on
    ])
    print(f"✅ Scraped Job ID: {job_id}")
    print(f" {job_name}, {company}, {location}, {job_type}, {batch}, {stream_required}, {salary}, {updated_on}")

# Close browser
driver.quit()

# File Name
file_name = "job_listings_with_details.csv"

# Save to CSV with hyperlinks
headers = ["Job ID", "Page Link", "Apply Link", "Job Title", "Company", 
           "Company Website", "Location", "Job Type", "Batch", "Stream Required", "Salary", "Date"]

df = pd.DataFrame(jobs, columns=headers)

# Convert links into Excel hyperlink format
df["Page Link"] = df["Page Link"].apply(lambda x: f'=HYPERLINK("{x}", "Job Details")' if x != "Not Available" else x)
df["Apply Link"] = df["Apply Link"].apply(lambda x: f'=HYPERLINK("{x}", "Apply Here")' if x != "Apply Link Not Available" else x)

# Check if the file exists before writing
file_exists = os.path.exists(file_name)

# Append new data without overwriting, avoiding duplicate headers
df.to_csv(file_name, index=False, quoting=1, encoding="utf-8-sig", mode='a', header=not file_exists)

print("\n📁 Scraping complete! New rows added to 'job_listings_with_details.csv'.'")
