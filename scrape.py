import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
import time
import re
import queue
import os

# Mapping French stats to English column names
STATS_TRANSLATIONS = {
    "Essais accordés": "Tries Scored",
    "Possession de la balle": "Possession (%)",
    "Occupation": "Territory (%)",
    "Mêlées gagnées": "Scrums Won",
    "Touches gagnées sur son propre lancer": "Lineouts Won (Own)",
    "Pénalités réussies": "Penalties Scored",
    "Pénalités concédées": "Penalties Conceded", 
    "Plaquages réussis": "Tackles Completed",
    "Plaquages manqués": "Missed Tackles",
}

# Global queue and storage
task_queue = queue.Queue()
all_data = []
total_matches_scraped = 0

# Locks to prevent printing or writing collisions
print_lock = threading.Lock()
results_lock = threading.Lock()

def clean_value(val):
    if not val: return 0
    return val.replace('%', '').replace('\xa0', '').strip()

def extract_date_time(text_string):
    if not text_string: return None, None
    date_match = re.search(r"(\d{2}/\d{2}/\d{4})", text_string)
    time_match = re.search(r"(\d{1,2}h\d{2})", text_string)
    return (date_match.group(1) if date_match else None, 
            time_match.group(1) if time_match else None)

def get_clean_urls(raw_url):
    # Strip any sub-paths to get the base match ID, then rebuild proper endpoints
    root = raw_url.split("?")[0]
    for suffix in ["/statistiques-du-match", "/compositions", "/resume", "/fil-du-match"]:
        if root.endswith(suffix):
            root = root.replace(suffix, "")
    return f"{root}/statistiques-du-match", f"{root}/compositions"

def create_driver():
    # Setup headless Chrome with options to avoid bot detection
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Disable automation flags
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Block images to speed up loading
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def scrape_phase_data(driver, season, phase):
    # Main logic to scrape a specific week (Day/Phase)
    global total_matches_scraped
    phase_data = []

    url = f"https://top14.lnr.fr/calendrier-et-resultats/{season}/{phase}"
    try:
        driver.get(url)
        time.sleep(2)
    except:
        with print_lock: print(f"[!] [{season} {phase}] Timeout loading calendar.")
        return []

    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a.match-links__link[title='Feuille de match']")
        match_urls = list(set([link.get_attribute('href') for link in links]))
    except:
        return []
    
    for i, raw_link in enumerate(match_urls):
        row = {}
        stats_count = 0
        stats_url, compo_url = get_clean_urls(raw_link)

        # 1. Get Match Stats
        try:
            driver.get(stats_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "match-header__title")))
            
            home_team = driver.find_element(By.CSS_SELECTOR, ".match-header-club__wrapper--left .match-header-club__title").get_attribute("textContent").strip()
            away_team = driver.find_element(By.CSS_SELECTOR, ".match-header-club__wrapper--right .match-header-club__title").get_attribute("textContent").strip()
            score_text = driver.find_element(By.CSS_SELECTOR, ".match-header__title .title").get_attribute("textContent").strip()
            h_score, a_score = map(int, score_text.split("-"))
            
            meta_text = driver.find_element(By.CLASS_NAME, "match-header__season-day").get_attribute("textContent").strip()
            match_date, match_time = extract_date_time(meta_text)

            row = {
                "Season": season, "Phase": phase, "Date": match_date, "Time": match_time,
                "Home_Team": home_team, "Away_Team": away_team, 
                "Home_Score": h_score, "Away_Score": a_score,
                "Winner": home_team if h_score > a_score else (away_team if a_score > h_score else "Draw"),
                "Referee": "Unknown",
                "Home_Lineup": "", "Away_Lineup": ""
            }

            # Loop through stat bars
            bars = driver.find_elements(By.CSS_SELECTOR, ".stats-bar")
            for bar in bars:
                try:
                    title = bar.find_element(By.CSS_SELECTOR, ".stats-bar__title").get_attribute("textContent").strip()
                    if title in STATS_TRANSLATIONS:
                        en_title = STATS_TRANSLATIONS[title]
                        val_left = bar.find_element(By.CSS_SELECTOR, ".stats-bar__val--left").get_attribute("textContent")
                        val_right = bar.find_element(By.CSS_SELECTOR, ".stats-bar__val--right").get_attribute("textContent")
                        row[f"Home_{en_title}"] = clean_value(val_left)
                        row[f"Away_{en_title}"] = clean_value(val_right)
                        stats_count += 1
                except: continue

        except Exception:
            continue

        # 2. Get Lineups and Referee
        driver.get(compo_url)
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "line-up__classic")))
            classic_container = driver.find_element(By.CLASS_NAME, "line-up__classic")
            
            team_blocks = classic_container.find_elements(By.CSS_SELECTOR, ".line-up__classic-team:not(.line-up__classic-team--officials)")
            is_visiting_first = "line-up__classic--visiting-first" in classic_container.get_attribute("class")
            
            if len(team_blocks) >= 2:
                t1_names = [p.get_attribute("textContent").strip() for p in team_blocks[0].find_elements(By.CLASS_NAME, "player-block__name")]
                t2_names = [p.get_attribute("textContent").strip() for p in team_blocks[1].find_elements(By.CLASS_NAME, "player-block__name")]
                
                # Check formatting to assign home/away correctly
                if is_visiting_first:
                    row["Away_Lineup"] = ", ".join(t1_names)
                    row["Home_Lineup"] = ", ".join(t2_names)
                    p_debug = f"H:{len(t2_names)}/A:{len(t1_names)}"
                else:
                    row["Home_Lineup"] = ", ".join(t1_names)
                    row["Away_Lineup"] = ", ".join(t2_names)
                    p_debug = f"H:{len(t1_names)}/A:{len(t2_names)}"
            else:
                p_debug = "0 Blocks"

            # Grab Referee
            try:
                ref_block = classic_container.find_element(By.CSS_SELECTOR, ".line-up__classic-team--officials")
                officials = ref_block.find_elements(By.CLASS_NAME, "player-block")
                for off in officials:
                    pos = off.find_element(By.CLASS_NAME, "player-block__position").get_attribute("textContent")
                    if "Arbitre" in pos:
                        row["Referee"] = off.find_element(By.CLASS_NAME, "player-block__name").get_attribute("textContent").strip()
                        break
            except: pass 

        except Exception as e:
            error_msg = str(e).split('\n')[0]
            p_debug = f"FAIL: {error_msg}"

        phase_data.append(row)
        
        with print_lock:
            total_matches_scraped += 1
            print(f"   [OK] [{season} {phase}] {home_team} vs {away_team}")
            print(f"        Stats: {stats_count} | Ref: {row['Referee']} | Lineups: {p_debug}")
            print("-" * 40)
            
    return phase_data

def worker_thread(worker_id):
    # Worker process: grabs tasks from queue until empty
    print(f"[Worker {worker_id}] Starting Browser...")
    driver = create_driver()
    
    while True:
        try:
            season, phase = task_queue.get(block=False)
        except queue.Empty:
            break
        
        try:
            data = scrape_phase_data(driver, season, phase)
            with results_lock:
                all_data.extend(data)
        except Exception as e:
            with print_lock:
                print(f"[ERR] Worker {worker_id} Failed on {season}-{phase}: {e}")
        finally:
            task_queue.task_done()
            
    print(f"[Worker {worker_id}] Finished. Closing Browser.")
    driver.quit()

def main():
    # 1. Populate the Queue
    seasons = ["2020-2021","2021-2022","2022-2023", "2023-2024", "2024-2025", "2025-2026"]
    playoff_phases = ["barrage", "demi-finale", "finale", "access-top-14", "match-daccession"]
    
    print("--- Setting up tasks ---")
    for s in seasons:
        # Stop at day 12 for the current season, otherwise go to 26
        limit = 12 if s == "2025-2026" else 27
        for j in range(1, limit):
            task_queue.put((s, f"j{j}"))
            
        # Add playoffs for completed seasons
        if s != "2025-2026":
            for phase in playoff_phases:
                task_queue.put((s, phase))
    
    print(f"Total Tasks in Queue: {task_queue.qsize()}")
    
    # 2. Launch Threads (change num_workers for more threads (dont blow up your computer))
    num_workers = 4
    threads = []
    
    print(f"Starting {num_workers} workers...")
    for i in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(i+1,))
        t.start()
        threads.append(t)
        time.sleep(1) 

    # 3. Wait for completion
    for t in threads:
        t.join()

    # 4. Export
    if all_data:
        df = pd.DataFrame(all_data)
        filename = "Top14_Raw_Scrape.csv"
        
        # Normalize columns
        for k in STATS_TRANSLATIONS.values():
            if f"Home_{k}" not in df.columns: df[f"Home_{k}"] = 0
            if f"Away_{k}" not in df.columns: df[f"Away_{k}"] = 0
            
        df.to_csv(filename, index=False)
        print(f"\n[Success] Saved {len(df)} matches to {filename}")
    else:
        print("\n[!] No data collected.")

if __name__ == "__main__":
    main()