Top 14 Rugby Predictor 🏉 🤖
This project uses machine learning to predict the outcomes of Top 14 rugby matches. It consists of a multi-threaded web scraper to gather historical match data from lnr.fr and a Random Forest classifier that analyzes team form and individual player lineups to forecast winners.

Features
1. Data Collection (scrape.py)
Multi-threaded Scraping: Uses Python threading and queue to scrape multiple match weeks in parallel, significantly reducing runtime.

Comprehensive Stats: Extracts match scores, tries, possession, territory, tackle counts, and penalties.

Lineup Extraction: Scrapes the full starting XV and bench for every match to analyze squad rotation.

Anti-Detection: Implements headless Chrome options and user-agent rotation to avoid bot detection.

2. Prediction Engine (predictor.py)
Random Forest Model: Uses a robust ensemble method to classify match outcomes (Home Win vs. Away Win).

Team Form Metrics: Calculates rolling 5-game averages for game points and score differentials.

Dynamic Player Strength: Implements a Bayesian Average system that tracks every player's win/loss record over time. The model assesses the strength of a specific lineup based on the historical performance of the players named in the squad.

Backtesting: Allows you to test the model's accuracy on specific date ranges.

Prerequisites
Ensure you have Google Chrome installed on your machine, as the scraper utilises the Chrome WebDriver.

Install the required Python packages:
pip install pandas selenium scikit-learn numpy
Usage

Step 1: Gather Data
Run the scraper to collect historical match data. By default, this covers seasons from 2020 to the current 2025-2026 season.

python scrape.py
Output: Generates Top14_Raw_Scrape.csv.
Note: Depending on your internet connection and the number of threads (default is 4), this may take a while.

Step 2: Train & Predict
Once the CSV file is generated, run the predictor.
python predictor.py
Configuration: You can adjust the training/testing date range in the if __name__ == "__main__": block at the bottom of predictor.py.

Output:
Prints accuracy score and match predictions to the console.
Saves detailed results to final_predictions.csv.

Project Structure
scrape.py: Selenium-based script for data extraction. Handles DOM parsing and data cleaning.
predictor.py: Handles feature engineering (calculating rest days, form, player strength) and model training.
Top14_Raw_Scrape.csv: The dataset produced by the scraper (input for the predictor).
final_predictions.csv: The final output file containing probabilities and confidence levels for predicted matches.

Methodology: "Player Strength"
A key feature of this model is how it handles lineups. Instead of treating teams as static entities, it calculates a Lineup_Strength score:

Tracking: Every time a player appears in a lineup, the model tracks if the team won or lost.

Bayesian Smoothing: To prevent outliers (e.g., a rookie winning their only game having a 100% win rate), player scores are smoothed toward a global mean.

Aggregation: For a new match, the model averages the smoothed scores of all players in the starting lineup to generate a dynamic strength metric for that specific game day.
