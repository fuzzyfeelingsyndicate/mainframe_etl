import os
import psycopg2
import random 

id_random = random.randint(1,10000)

DBNAME = os.getenv("dbname")

def main(id):

    conn = psycopg2.connect(
       user=os.getenv('SUPABASE_USER'),
       password= os.getenv('SUPABASE_PASS'),
       host=os.getenv('SUPABASE_HOST'),
       port=6543,
       dbname=os.getenv('SUPABASE_DB')
    )
    cur = conn.cursor()

    print('cannot connect')

    cur.execute('''insert into "testTable" (id, name) 
    values(%s, %s)''', (id, f'test{id}'))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main(id_random)

