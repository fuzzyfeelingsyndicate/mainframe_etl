import os
from datetime import datetime, timedelta
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_odds_history():
    try:
        six_hours_ago = datetime.utcnow() - timedelta(hours=170)
        
        response = supabase.table("odds_history").delete().lt(
            "pulled_at", six_hours_ago.isoformat()
        ).execute()
        
        print(f"✓ Cleaned odds_history: {len(response.data)} records deleted")
        return True
    except Exception as e:
        print(f"✗ Error cleaning odds_history: {str(e)}")
        return False

def clean_markets():
    try:
        current_time = datetime.utcnow().isoformat()
        
        response = supabase.table("markets").delete().lt(
            "created_at", current_time
        ).execute()
        
        print(f"✓ Cleaned markets: {len(response.data)} records deleted")
        return True
    except Exception as e:
        print(f"✗ Error cleaning markets: {str(e)}")
        return False

def clean_events():
    try:
        current_time = datetime.utcnow().isoformat()
        
        response = supabase.table("events").delete().lt(
            "created_at", current_time
        ).execute()
        
        print(f"✓ Cleaned events: {len(response.data)} records deleted")
        return True
    except Exception as e:
        print(f"✗ Error cleaning events: {str(e)}")
        return False

def main():
    """Execute cleaning in the correct order"""
    print("Starting database cleanup...\n")
    
    if not clean_odds_history():
        print("Stopping cleanup due to odds_history error")
        return
    
    if not clean_markets():
        print("Stopping cleanup due to markets error")
        return
    
    if not clean_events():
        print("Stopping cleanup due to events error")
        return
    
    print("\n✓ Database cleanup completed successfully!")

if __name__ == "__main__":
    main()
