Top 14 Rugby Predictor 🏉 🤖
This project uses machine learning to predict the outcomes of Top 14 rugby matches. It consists of a multi-threaded web scraper to gather historical and future match data from lnr.fr, and a Random Forest classifier that analyzes team form and individual player lineups to forecast winners.

Features
1. Data Collection (scrape.py)
Smart Incremental Scraping: automatically checks your existing CSV before running. It skips completed weeks (e.g., if J1-J26 are already scraped, it won't touch them), only scraping new or incomplete rounds.

Future Match Support: Capable of scraping scheduled games that haven't happened yet. It captures the date, teams, and lineups (if available) while gracefully handling missing scores and stats.

Self-Correcting Database: When a future game is re-scraped after it has been played, the scraper automatically updates the entry with the final score and stats, removing the old "future" placeholder.

Multi-threaded: Uses Python threading and a queue to scrape multiple match weeks in parallel, significantly reducing runtime.

Lineup Extraction: Scrapes the full starting XV and bench for every match to analyze squad rotation.

2. Prediction Engine (predictor.py)
Future Forecasting: Automatically detects games in the dataset that have no result (future games). It uses all past data to train the model and then outputs predictions specifically for these upcoming fixtures.

Random Forest Model: Uses a robust ensemble method to classify match outcomes (Home Win vs. Away Win).

Dynamic Player Strength: Implements a Bayesian Average system that tracks every player's win/loss record over time.

Team Form Metrics: Calculates rolling 5-game averages for game points, score differentials, and rest days.

Prerequisites
Ensure you have Google Chrome installed, as the scraper utilizes the Chrome WebDriver.

Install the required Python packages:
pip install pandas selenium scikit-learn numpy
Usage

Step 1: Gather/Update Data
Run the scraper to collect match data.
python scrape.py
First Run: Will scrape everything from 2020 to present (may take a few minutes).

Subsequent Runs: Will only scrape new weeks or the current incomplete week.

Output: Updates Top14_Raw_Scrape.csv.

Step 2: Train & Predict
Run the predictor to train the model and generate forecasts.
python predictor.py

Logic: The script splits the data into "Past" (games with scores) and "Future" (games without scores). It trains on the past and predicts the future.

Output:

Prints accuracy scores (backtesting) and predictions for upcoming games to the console.

Saves detailed results to final_predictions.csv.

The Weekly Workflow (Self-Correcting Cycle)
This system is designed to be run weekly without changing code.

Before the Match (Friday):

Run scrape.py. It sees "J12" has no score but has lineups. It saves the game with Winner = NaN.

Run predictor.py. It sees Winner = NaN, skips training on this row, and uses the model to predict the winner based on the lineups.

After the Match (Monday):

Run scrape.py. It re-scrapes "J12", sees the final score (e.g., 24-10), and overwrites the old entry in the CSV.

Run predictor.py. It sees Winner = Home. It now trains on this game, updating the "Player Strength" memory for the players involved, making the model smarter for J13.

Methodology: "Player Strength"
A key feature of this model is how it handles lineups. Instead of treating teams as static entities, it calculates a Lineup_Strength score:

Tracking: Every time a player appears in a lineup, the model tracks if the team won or lost.

Bayesian Smoothing: To prevent outliers (e.g., a rookie winning their only game having a 100% win rate), player scores are smoothed toward a global mean (0.5).

Aggregation: For a new match, the model averages the smoothed scores of all players in the starting lineup to generate a dynamic strength metric for that specific game day.

Project Structure
scrape.py: Smart Selenium scraper. Handles deduplication, incremental updates, and DOM parsing.

predictor.py: Feature engineering (rest days, form, player strength) and Random Forest training/inference.

Top14_Raw_Scrape.csv: The master database. Grows over time as you scrape new weeks.

final_predictions.csv: The output file containing probabilities and confidence levels.
