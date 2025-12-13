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
import numpy as np

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

# Locks
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
    root = raw_url.split("?")[0]
    for suffix in ["/statistiques-du-match", "/compositions", "/resume", "/fil-du-match"]:
        if root.endswith(suffix):
            root = root.replace(suffix, "")
    return f"{root}/statistiques-du-match", f"{root}/compositions"

def create_driver():
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
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def scrape_phase_data(driver, season, phase):
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
            
            # Handle Future Games
            try:
                score_text = driver.find_element(By.CSS_SELECTOR, ".match-header__title .title").get_attribute("textContent").strip()
                if "-" in score_text:
                    h_score, a_score = map(int, score_text.split("-"))
                    winner = home_team if h_score > a_score else (away_team if a_score > h_score else "Draw")
                else:
                    raise ValueError("Future Game")
            except:
                h_score, a_score = 0, 0
                winner = None # Flags this as a future game
            
            meta_text = driver.find_element(By.CLASS_NAME, "match-header__season-day").get_attribute("textContent").strip()
            match_date, match_time = extract_date_time(meta_text)

            row = {
                "Season": season, "Phase": phase, "Date": match_date, "Time": match_time,
                "Home_Team": home_team, "Away_Team": away_team, 
                "Home_Score": h_score, "Away_Score": a_score,
                "Winner": winner,
                "Referee": "Unknown",
                "Home_Lineup": "", "Away_Lineup": ""
            }

            if winner: # Only scrape stats bars if game happened
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
            # Mark future games clearly in print output
            status_tag = "[OK]" if row['Winner'] else "[FUTURE]"
            print(f"   {status_tag} [{season} {phase}] {home_team} vs {away_team}")
            print(f"        Stats: {stats_count} | Ref: {row['Referee']} | Lineups: {p_debug}")
            print("-" * 40)
            
    return phase_data

def worker_thread(worker_id):
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

def get_completed_phases(filename):
    """Returns a set of (Season, Phase) tuples that are already fully scraped."""
    if not os.path.exists(filename): 
        return set()
    
    print(f"Checking {filename} for existing data...")
    df = pd.read_csv(filename)
    
    # Only count rows where we actually have a winner (game is over)
    finished_games = df[df['Winner'].notna()]
    
    # Count completed games per phase
    counts = finished_games.groupby(['Season', 'Phase']).size()
    
    # Define expected game counts
    # Regular season (jX) usually has 7. Playoffs have specific counts.
    PLAYOFF_COUNTS = {
        "barrage": 2,
        "demi-finale": 2,
        "finale": 1,
        "access-top-14": 1,
        "match-daccession": 1
    }

    completed = set()
    for (season, phase), count in counts.items():
        if phase.startswith('j'):
            if count >= 7: # Regular season standard
                completed.add((season, phase))
        elif phase in PLAYOFF_COUNTS:
            if count >= PLAYOFF_COUNTS[phase]: # Playoff standard
                completed.add((season, phase))
            
    return completed

def main():
    seasons = ["2020-2021","2021-2022","2022-2023", "2023-2024", "2024-2025", "2025-2026"]
    playoff_phases = ["barrage", "demi-finale", "finale", "access-top-14", "match-daccession"]
    filename = "Top14_Raw_Scrape.csv"

    # 1. Identify what to skip
    completed_phases = get_completed_phases(filename)
    print(f"Skipping {len(completed_phases)} previously completed phases.")

    # 2. Populate the Queue
    print("--- Setting up tasks ---")
    for s in seasons:
        # Increase limit to include the future round (e.g., 14 to get J13)
        limit = 13 if s == "2025-2026" else 27
        for j in range(1, limit):
            phase_id = f"j{j}"
            
            # --- SKIP LOGIC ---
            if (s, phase_id) in completed_phases:
                continue
            
            task_queue.put((s, phase_id))
            
        if s != "2025-2026":
            for phase in playoff_phases:
                if (s, phase) in completed_phases:
                    continue
                task_queue.put((s, phase))
    
    print(f"Total Tasks in Queue: {task_queue.qsize()}")
    
    if task_queue.qsize() == 0:
        print("Nothing new to scrape.")
        return

    # 3. Launch Threads
    num_workers = 4
    threads = []
    
    print(f"Starting {num_workers} workers...")
    for i in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(i+1,))
        t.start()
        threads.append(t)
        time.sleep(1) 

    for t in threads:
        t.join()

    # 4. Export (Smart Merge)
    if all_data:
        new_df = pd.DataFrame(all_data)
        
        # Normalize columns
        for k in STATS_TRANSLATIONS.values():
            if f"Home_{k}" not in new_df.columns: new_df[f"Home_{k}"] = 0
            if f"Away_{k}" not in new_df.columns: new_df[f"Away_{k}"] = 0
            
        if os.path.exists(filename):
            print(f"Merging with existing {filename}...")
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, new_df])
            
            # DEDUPLICATE: Keep the 'last' entry (the new one) for any Season/Phase/HomeTeam combo
            final_df = combined_df.drop_duplicates(subset=['Season', 'Phase', 'Home_Team'], keep='last')
            print(f"Merged: {len(existing_df)} old + {len(new_df)} new -> {len(final_df)} total unique.")
        else:
            final_df = new_df
            print(f"Created new file with {len(final_df)} matches.")
            
        final_df.to_csv(filename, index=False)
        print(f"\n[Success] Database updated.")
    else:
        print("\n[!] No data collected.")

if __name__ == "__main__":
    main()