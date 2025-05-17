from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import datetime
import concurrent.futures
from pymongo import MongoClient

# Configure WebDriver with minimal settings
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,720")

# MongoDB Connection
client = MongoClient('mongodb+srv://rohaXCUkaWw2@cluster0.munr4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['job_scraper_db']
collection = db['job_listings']

# Fast text extraction function
def get_text_safe(driver, xpath):
    try:
        return driver.find_element(By.XPATH, xpath).text.strip()
    except Exception:
        return "Not Available"

# Function to get HTML content by class
def get_html_content(driver, class_name):
    try:
        element = driver.find_element(By.CLASS_NAME, class_name)
        return element.get_attribute('innerHTML')
    except Exception:
        return "Not Available"

# Function to get text content by class
def get_text_content(driver, class_name):
    try:
        element = driver.find_element(By.CLASS_NAME, class_name)
        return element.text.strip()
    except Exception:
        return "Not Available"

# Get list of existing job IDs from MongoDB
existing_job_ids = set(doc['job_id'] for doc in collection.find({}, {"job_id": 1, "_id": 0}))
print(f"Found {len(existing_job_ids)} existing job IDs in database")

# Base URLs
details_base_url = "https://thejobcompany.in/frontend/job_details?job_id="
apply_base_url = "https://thejobcompany.in/frontend/apply_page.php?job_id="

# Process a batch of job IDs
def process_job_batch(job_id_batch):
    # Create a new driver instance for this batch
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)  # Short timeout for better speed
    
    batch_results = {
        'added': 0,
        'not_found': 0,
        'skipped_incomplete': 0,
        'added_ids': []
    }
    
    try:
        for job_id in job_id_batch:
            # Skip if job ID already exists in database
            if job_id in existing_job_ids:
                continue
                
            job_url = f"{details_base_url}{job_id}"
            apply_url = f"{apply_base_url}{job_id}"

            # Load job details page
            driver.get(job_url)
            
            # Check if "No job found for this ID" exists
            if "No job found for this ID." in driver.page_source:
                batch_results['not_found'] += 1
                continue

            # Extract job details
            job_name = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[1]")
            company = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[2]") 
            company_website = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[1]/div[1]/p[2]/a")
            location = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[2]/div[1]/p[2]")
            job_type = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[3]/div[1]/p[2]")
            batch = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[4]/div[1]/p[2]")
            stream_required = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[5]/div[1]/p[2]")
            salary = get_text_safe(driver, "/html/body/div[2]/div[1]/div[3]/div/div[6]/div[1]/p[2]")
            updated_on = get_text_safe(driver, "/html/body/div[2]/div[1]/div[1]/div/p[3]")
            
            # NEW: Extract job description content from jd-content div
            job_description_html = get_html_content(driver, "jd-content")
            job_description_text = get_text_content(driver, "jd-content")
            
            # Clean up updated_on field
            if "Updated On:" in updated_on:
                updated_on = updated_on[updated_on.find("Updated On:") + 11:].strip()

            # Get apply link
            driver.get(apply_url)
            try:
                apply_link = wait.until(EC.presence_of_element_located((By.ID, "applyButton"))).get_attribute("href")
            except:
                apply_link = "Apply Link Not Available"

            # Check if any important field is "Not Available" and skip
            important_fields = [job_name, company, location, job_type]
            
            if any(field == "Not Available" for field in important_fields):
                print(f"⚠️ Skipping Job ID {job_id} - Incomplete data detected")
                batch_results['skipped_incomplete'] += 1
                continue

            # Create job document for MongoDB
            job_document = {
                "job_id": job_id,
                "page_url": job_url,
                "apply_url": apply_link,
                "job_title": job_name,
                "company": company,
                "company_website": company_website,
                "location": location,
                "job_type": job_type,
                "batch": batch,
                "stream_required": stream_required,
                "salary": salary,
                "job_description_html": job_description_html,  # NEW: Add job description HTML
                "job_description_text": job_description_text,  # NEW: Add job description text
                "updated_on": updated_on,
                "scraped_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Insert job in MongoDB
            collection.insert_one(job_document)
            batch_results['added'] += 1
            batch_results['added_ids'].append(job_id)
            
    except Exception as e:
        print(f"Error in batch: {str(e)}")
    finally:
        driver.quit()
        return batch_results

# Main execution
def main():
    start_id = 1
    end_id = 4000
    
    # Filter out IDs that already exist in the database
    jobs_to_process = [job_id for job_id in range(start_id, end_id + 1) 
                      if job_id not in existing_job_ids]
    
    print(f"Processing {len(jobs_to_process)} new job IDs")
    
    # Create batches of job IDs (adjust batch size as needed)
    batch_size = 20
    job_batches = [jobs_to_process[i:i + batch_size] 
                  for i in range(0, len(jobs_to_process), batch_size)]
    
    total_added = 0
    total_not_found = 0
    total_skipped_incomplete = 0
    
    # Use thread pool for parallel processing
    max_workers = 5  # Adjust based on your system capabilities
    print(f"Starting {len(job_batches)} batches with {max_workers} parallel workers")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_job_batch, batch): i 
                          for i, batch in enumerate(job_batches)}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                result = future.result()
                total_added += result['added']
                total_not_found += result['not_found']
                total_skipped_incomplete += result.get('skipped_incomplete', 0)
                
                # Print simple progress
                print(f"Batch {batch_index+1}/{len(job_batches)} complete: "
                      f"Added {result['added']} jobs, "
                      f"Skipped {result.get('skipped_incomplete', 0)} incomplete")
                
                # Add newly processed IDs to our set
                for job_id in result['added_ids']:
                    existing_job_ids.add(job_id)
                    
            except Exception as exc:
                print(f"Batch {batch_index} failed with error: {exc}")
    
    # Create a unique index on job_id
    collection.create_index("job_id", unique=True)
    
    # Print summary
    print("\n📊 Scraping Summary:")
    print(f"✅ Total new jobs added: {total_added}")
    print(f"❌ Total IDs with no job found: {total_not_found}")
    print(f"⚠️ Total jobs skipped due to incomplete data: {total_skipped_incomplete}")
    print(f"✅ Data stored in MongoDB database 'job_scraper_db', collection 'job_listings'")
    
    # Close MongoDB connection
    client.close()

if __name__ == "__main__":
    main()
