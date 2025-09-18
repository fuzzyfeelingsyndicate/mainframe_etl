import os
import psycopg2
import random 

id_random = random.randint(1,10000)


def main(id):
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASS"),
        port=5432
    )
    cur = conn.cursor()

    cur.execute('''insert into "testTable" (id, name) 
    values(%s, %s)''', (id_random, f'test{id_random}'))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
