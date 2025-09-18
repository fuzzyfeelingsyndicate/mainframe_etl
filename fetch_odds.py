import os
import psycopg2

'''try:
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASS"),
        port=6543
    )
    print("✅ Connection successful")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)'''

def main():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASS"),
        port=5432
    )
    cur = conn.cursor()

    cur.execute('select * from "testTable"')

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
