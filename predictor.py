import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

def backtest_model(start_date, end_date):
    print("--- Starting Feature Engineering ---")
    
    # Load data and fix dates
    df = pd.read_csv('Top14_Raw_Scrape.csv')
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    df = df.sort_values('Date').reset_index(drop=True)
    
    # Target: 1 if Home Wins, 0 otherwise (Handle NaN for future games)
    df['Target'] = np.where(df['Winner'].notna(), (df['Winner'] == df['Home_Team']).astype(int), np.nan)
    
    # ---------------------------------------------------------
    # TEAM FORM FEATURES
    # ---------------------------------------------------------
    
    # Split into home/away perspectives to calculate history
    home_df = df[['Date', 'Home_Team', 'Home_Score', 'Away_Score']].copy()
    home_df.columns = ['Date', 'Team', 'Score', 'Opp_Score']
    
    away_df = df[['Date', 'Away_Team', 'Away_Score', 'Home_Score']].copy()
    away_df.columns = ['Date', 'Team', 'Score', 'Opp_Score']
    
    # Stack to create a single timeline per team
    long_df = pd.concat([home_df, away_df]).sort_values(['Team', 'Date'])
    
    # Points allocation: Win=1, Draw=0.5, Loss=0
    conditions = [
        long_df['Score'] > long_df['Opp_Score'],
        long_df['Score'] == long_df['Opp_Score']
    ]
    choices = [1.0, 0.5]
    long_df['Game_Points'] = np.select(conditions, choices, default=0.0)
    
    # Simple score difference
    long_df['Score_Diff'] = long_df['Score'] - long_df['Opp_Score']
    
    # Days since last match (fill NaNs with 30 for start of season)
    long_df['Last_Date'] = long_df.groupby('Team')['Date'].shift(1)
    long_df['Rest_Days'] = (long_df['Date'] - long_df['Last_Date']).dt.days
    long_df['Rest_Days'] = long_df['Rest_Days'].fillna(30)
    
    # Rolling 5-game averages
    metrics = ['Game_Points', 'Score_Diff']
    
    for m in metrics:
        long_df[f'Form_{m}'] = long_df.groupby('Team')[m].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        
    long_df = long_df.fillna(0)
    
    # Merge stats back into the main match dataframe
    cols_to_merge = ['Date', 'Team', 'Rest_Days', 'Form_Game_Points', 'Form_Score_Diff']
    
    # Home stats
    df = df.merge(long_df[cols_to_merge], left_on=['Date', 'Home_Team'], right_on=['Date', 'Team'], how='left')
    df = df.drop(columns=['Team'])
    df = df.rename(columns={'Rest_Days': 'H_Rest_Days', 'Form_Game_Points': 'H_Form_Points', 'Form_Score_Diff': 'H_Form_Diff'})
    
    # Away stats
    df = df.merge(long_df[cols_to_merge], left_on=['Date', 'Away_Team'], right_on=['Date', 'Team'], how='left')
    df = df.drop(columns=['Team'])
    df = df.rename(columns={'Rest_Days': 'A_Rest_Days', 'Form_Game_Points': 'A_Form_Points', 'Form_Score_Diff': 'A_Form_Diff'})

    # ---------------------------------------------------------
    # PLAYER STRENGT
    # ---------------------------------------------------------
    
    player_stats = {} 
    
    # Baseline settings for Bayesian smoothing
    global_mean = 0.5 
    C = 5 
    
    h_strength_list = []
    a_strength_list = []
    
    # Loop through games chronologically to prevent data leakage
    for idx, row in df.iterrows():
        
        # Calculate current lineup strength based on PAST games
        def get_weighted_strength(lineup_str):
            if pd.isna(lineup_str): return global_mean
            players = [p.strip() for p in str(lineup_str).split(',')]
            scores = []
            for p in players:
                stats = player_stats.get(p, {'sum_pts': 0, 'games': 0})
                # Bayesian Average
                smoothed_score = (stats['sum_pts'] + (C * global_mean)) / (stats['games'] + C)
                scores.append(smoothed_score)
            return np.mean(scores) if scores else global_mean

        h_strength_list.append(get_weighted_strength(row['Home_Lineup']))
        a_strength_list.append(get_weighted_strength(row['Away_Lineup']))
        
        # Post-game: Update player memory for the NEXT loop
        if pd.isna(row['Winner']): continue
        
        if row['Winner'] == row['Home_Team']:
            h_pts, a_pts = 1.0, 0.0
        elif row['Winner'] == 'Draw':
            h_pts, a_pts = 0.5, 0.5
        else:
            h_pts, a_pts = 0.0, 1.0
            
        def update_player_memory(lineup_str, points):
            if pd.isna(lineup_str): return
            for p in [x.strip() for x in str(lineup_str).split(',')]:
                s = player_stats.get(p, {'sum_pts': 0, 'games': 0})
                
                # Decay old stats slightly
                s['sum_pts'] *= 0.99 
                s['games'] *= 0.99
                
                s['sum_pts'] += points
                s['games'] += 1
                player_stats[p] = s
        
        update_player_memory(row['Home_Lineup'], h_pts)
        update_player_memory(row['Away_Lineup'], a_pts)
        
    df['H_Lineup_Strength'] = h_strength_list
    df['A_Lineup_Strength'] = a_strength_list
    
    # ---------------------------------------------------------
    # MODEL TRAINING
    # ---------------------------------------------------------
    
    # Feature diffs usually correlate better than raw values
    df['Strength_Diff'] = df['H_Lineup_Strength'] - df['A_Lineup_Strength']
    df['Rest_Diff'] = df['H_Rest_Days'] - df['A_Rest_Days']
    df['Form_Point_Diff'] = df['H_Form_Points'] - df['A_Form_Points']
    
    features = [
        'H_Lineup_Strength', 'A_Lineup_Strength', 'Strength_Diff',
        'H_Rest_Days', 'A_Rest_Days', 'Rest_Diff',
        'H_Form_Points', 'A_Form_Points', 'Form_Point_Diff',
        'H_Form_Diff', 'A_Form_Diff'
    ]
    train_data = df[df['Winner'].notna()].dropna(subset=features)
    future_data = df[df['Winner'].isna()].dropna(subset=features)

    print(f"Training on {len(train_data)} past games.")
    
    clf = RandomForestClassifier(n_estimators=300, max_depth=10, min_samples_leaf=3, random_state=1)
    clf.fit(train_data[features], train_data['Target'])
    
    # Predict Future
    if not future_data.empty:
        print(f"Predicting {len(future_data)} future games...")
        future_preds = clf.predict(future_data[features])
        future_probs = clf.predict_proba(future_data[features])[:, 1]
        
        future_data['Predicted_Home_Win'] = future_preds
        future_data['Home_Win_Probability'] = future_probs
        future_data['Correct'] = False # Placeholder
        
        return future_data[['Date', 'Home_Team', 'Away_Team', 'Winner', 'Home_Win_Probability', 'Correct']]
    
    # Fallback to standard testing if no future games found
    test_data = df[(df['Date'] >= pd.to_datetime(start_date, dayfirst=True)) & 
                   (df['Date'] <= pd.to_datetime(end_date, dayfirst=True))].dropna(subset=features)
                   
    preds = clf.predict(test_data[features])
    probs = clf.predict_proba(test_data[features])[:, 1]
    
    test_data['Predicted_Home_Win'] = preds
    test_data['Home_Win_Probability'] = probs
    test_data['Correct'] = (test_data['Predicted_Home_Win'] == test_data['Target'])
    
    acc = accuracy_score(test_data['Target'], preds)
    print(f"\n--- Backtest Accuracy: {acc:.2%} ---")
    
    return test_data[['Date', 'Home_Team', 'Away_Team', 'Winner', 'Home_Win_Probability', 'Correct']]

# ---------------------------------------------------------
# RUN
# ---------------------------------------------------------
if __name__ == "__main__":
    # Adjust dates as needed based on available data
    results = backtest_model('06/10/2025', '30/11/2025')
    
    if results is not None:
        # Determine who we think won
        results['Predicted_Winner'] = np.where(
            results['Home_Win_Probability'] > 0.5, 
            results['Home_Team'], 
            results['Away_Team']
        )
        
        # Calculate how confident we are
        results['Confidence'] = np.where(
            results['Home_Win_Probability'] > 0.5,
            results['Home_Win_Probability'],
            1 - results['Home_Win_Probability']
        )
        
        # Formatting for print output
        print_df = results.copy()
        print_df['Date'] = print_df['Date'].dt.strftime('%d/%m/%Y')
        print_df['Confidence'] = (print_df['Confidence'] * 100).map('{:.1f}%'.format)
        print_df['Outcome'] = np.where(print_df['Correct'], 'Correct', 'WRONG')
        
        cols_to_show = ['Date', 'Home_Team', 'Away_Team', 'Predicted_Winner', 'Confidence', 'Winner', 'Outcome']
        
        print("\n--- Predictions ---")
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 1000)
        
        print(print_df[cols_to_show].to_string(index=False))
        
        results.to_csv('final_predictions.csv', index=False)
        print("\nSaved to 'final_predictions.csv'")