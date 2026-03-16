import pandas as pd
from pathlib import Path

RAW_DIR = Path('data/f5')
CLEANED_DIR = Path('data/cleaned')
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

COLUMNS_TO_DROP = {
    'messages': [
        'category',
        'platform',
        'soft_bounced_at',
        'complained_at',
        'blocked_at',
        'hard_bounced_at',
        'unsubscribed_at',
        'clicked_first_time_at',
        'clicked_last_time_at',
        'purchased_at'
    ],
    'campaigns': [
        'ab_test',
        'hour_limit',
        'position'
    ]
}


def load_csv(filename: str) -> pd.DataFrame:
    filepath = RAW_DIR / filename
    for encoding in ['utf-8', 'cp1251', 'latin1']:
        try:
            return pd.read_csv(filepath, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Failed to read {filename}")


def clean_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    df_clean = df.copy()
    
    if table_name in COLUMNS_TO_DROP:
        cols = [c for c in COLUMNS_TO_DROP[table_name] if c in df_clean.columns]
        if cols:
            df_clean = df_clean.drop(columns=cols)
    
    return df_clean


def main():
    print("Data cleaning started...")
    
    files = {
        'campaigns': 'campaigns.csv',
        'events': 'events.csv',
        'friends': 'friends.csv',
        'messages': 'messages.csv',
        'client_first_purchase_date': 'client_first_purchase_date.csv'
    }
    
    for name, filename in files.items():
        print(f"\n{name}:")
        try:
            df = load_csv(filename)
            print(f"Loaded: {len(df)} rows, {len(df.columns)} columns")
            
            df_clean = clean_data(df, name)
            
            print(f"Cleaned: {len(df_clean)} rows, {len(df_clean.columns)} columns")
            
            df_clean.to_csv(CLEANED_DIR / filename, index=False, encoding='utf-8')
            print(f"Saved: {CLEANED_DIR / filename}")
            
        except FileNotFoundError:
            print(f"File not found: {filename}")
        except Exception as e:
            print(f"Error: {e}")
    
    print("\n" + "=" * 60)

if __name__ == '__main__':
    main()
