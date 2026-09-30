import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    CREATE SECRET r2 (TYPE r2,
      KEY_ID '{os.environ["R2_KEY_ID"]}',
      SECRET '{os.environ["R2_SECRET"]}',
      ACCOUNT_ID '{os.environ["R2_ACCOUNT_ID"]}')
""")

df = con.sql("""
    SELECT * FROM read_csv('r2://econ-raw/ce.series.txt', delim = '\t')
    LIMIT 10
""").df()

print(df)