import pandas as pd
from pathlib import Path

RAW_DIR = Path('data/f5')

def load_csv(filename: str) -> pd.DataFrame:
    filepath = RAW_DIR / filename
    for encoding in ['utf-8', 'cp1251', 'latin1']:
        try:
            return pd.read_csv(filepath, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"File is incorrect: {filename}")

def main():
    print("=" * 90)
    print("Data null checking")
    print("=" * 90)
    
    files = {
        'campaigns': 'campaigns.csv',
        'events': 'events.csv',
        'friends': 'friends.csv',
        'messages': 'messages.csv',
        'client_first_purchase_date': 'client_first_purchase_date.csv'
    }
    
    for name, filename in files.items():
        try:
            df = load_csv(filename)
            print(f"\nTable: {name} ({len(df)} rows)")
            print("-" * 90)
            print(f"{'Column':<35} | {'Null Count':>12} | {'Null %':>10}")
            print("-" * 90)
            
            for col in df.columns:
                null_count = df[col].isnull().sum()
                null_percent = round((null_count / len(df)) * 100, 2)
                print(f"{col:<35} | {null_count:>12} | {null_percent:>10}%")
                
        except FileNotFoundError:
            print(f"Not found: {filename}")
        except Exception as e:
            print(f"Error with {filename}: {e}")
    
    print("\n" + "=" * 90)

if __name__ == '__main__':
    main()
