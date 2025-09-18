import os
#import psycopg2

print(os.getenv("SUPABASE_HOST"))
print(os.getenv("SUPABASE_DB"))
print(os.getenv("SUPABASE_USER"))
print(os.getenv("SUPABASE_PASS"))



'''def main():
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
    main() '''
