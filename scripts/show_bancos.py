import psycopg2
import os

def main():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM bancos LIMIT 5")
    rows = cur.fetchall()
    print("\n" + "="*80)
    print("=== EVIDENCIA 2: RESULTADO DO BANCO DE DADOS (TOP 5) ===")
    print("="*80)
    print(f"{'CNPJ':<20} | {'Seg.':<4} | {'Nome da Instituição'}")
    print("-" * 80)
    for r in rows:
        print(f"{r[0]:<20} | {r[1]:<4} | {r[2]}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
