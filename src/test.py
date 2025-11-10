import pandas as pd

# Function to read and show a CSV file
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df  
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def show_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        print("CSV File Content:") 
        print(df)
        return df 
    except Exception as e:
        print(f"Error showing CSV file: {e}")
        return None   


# Function to read and show a Parquet file
def read_parquet_file(file_path):
    try:
        df = pd.read_parquet(file_path)
        return df 
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None
    
def show_parquet_file(file_path):
    try:
        df = pd.read_parquet(file_path)
        print("Parquet File Content:") 
        print(df)
        return df 
    except Exception as e:
        print(f"Error showing Parquet file: {e}")
        return None    

# Example 
csv_file_path = "data/merged_data.csv"  
parquet_file_path = "data/merged_data.parquet"

# อ่านไฟล์ Parquet
merged = read_parquet_file(parquet_file_path)  

if merged is not None:
    # จำนวน row และ NaN
    print("\n--- Row - NaN ---")
    print("จำนวน row ทั้งหมดใน birth_country:", merged['birth_country'].count())
    print("จำนวน NaN ใน birth_country:", merged['birth_country'].isna().sum())
    
    # นับจำนวนแต่ละค่า birth_country ก่อน
    country_counts = merged['birth_country'].value_counts()
    
    print("\n--- Birth Country Counts (All) ---")
    print(country_counts)
      
print("\n--- Example Data ---")
merged = show_parquet_file(parquet_file_path)  
    
