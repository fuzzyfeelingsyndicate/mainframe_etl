import os
from datetime import datetime, timedelta
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

from datetime import datetime, timedelta
from supabase import create_client
import os

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)


def clean_old_events():
    try:
        cutoff = datetime.utcnow() - timedelta(hours=6)

        response = supabase.table("events").delete().lt(
            "start_time", cutoff.isoformat()
        ).execute()

        print(f"✓ Deleted events: {len(response.data)}")

        return True

    except Exception as e:
        print(f"✗ Cleanup error: {str(e)}")
        return False


def main():
    print("Starting cleanup...\n")

    if not clean_old_events():
        print("Cleanup failed")
        return

    print("\n✓ Cleanup completed successfully!")


if __name__ == "__main__":
    main()
