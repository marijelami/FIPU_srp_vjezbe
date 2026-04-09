'''
Skripta za generiranje dimenzijskog modela podataka -> star schema
Ova skripta demonstrira cijeli proces:
1. Konekcija na bazu podataka
2. Definicija modela (dimenzije i činjenice)
3. Brisanje starih tablica
4. Kreiranje novih tablica
5. ETL proces - punjenje podacima
6. Verifikacija rezultata
'''

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sqlalchemy import text

# Konekcija na bazu podataka
DATABASE_URL = "mysql+pymysql://root:1234@localhost:3306/dw"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# ========== DEFINICIJA DIMENZIJSKIH I FACT TABLICA ==========
# U dimenzijskom modelu (star schema):
# - Dimenzijske tablice (dim_) sadrže opisne atribute - TKO, ŠTO, GDJE, KADA
# - Fact tablice (fact_) sadrže mjerljive numeričke vrijednosti - KOLIKO, VRIJEDNOST, MARGINA

# Definicija dimenzijskih tablica i fact tablica
class DimCountry(Base):
    __tablename__ = 'dim_country'
    __table_args__ = {'schema': 'dw'}

    country_tk = Column(BigInteger, primary_key=True, autoincrement=True) # country_tk - surogatni ključ (Surrogate Key), umjetni primarni ključ - Koristi se umjesto prirodnog ključa (country_id) radi bolje kontrole i performansi
    version = Column(Integer)  
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    # Poslovni atributi dimenzije
    country_id = Column(Integer, index=True) # Prirodni ključ iz izvornog sustava
    name = Column(String(45))  # Naziv države
    population = Column(Integer) # Broj stanovnika
    region = Column(String(45)) # Regija kojoj država pripada


class DimProduct(Base):
    __tablename__ = 'dim_product'
    __table_args__ = {'schema': 'dw'}

    product_tk = Column(BigInteger, primary_key=True, autoincrement=True) # Surogatni ključ
    version = Column(Integer)
    date_from = Column(DateTime)   # SCD Type 2 - praćenje promjena kroz vrijeme
    date_to = Column(DateTime)
    product_id = Column(Integer, index=True) 
    product_name = Column(String(256)) # Naziv pojedinačnog proizvoda
    product_type_name = Column(String(256))
    product_line_name = Column(String(256))


class DimRetailer(Base):
    __tablename__ = 'dim_retailer'
    __table_args__ = {'schema': 'dw'}

    retailer_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    retailer_id = Column(Integer, index=True)
    name = Column(String(256))
    speciality_store = Column(Integer)


class DimOrderMethod(Base):
    __tablename__ = 'dim_order_method'
    __table_args__ = {'schema': 'dw'}

    order_method_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    order_method_id = Column(Integer, index=True)
    name = Column(String(45))


class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'dw'}

    date_tk = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)


class FactSales(Base):
    __tablename__ = 'fact_sales'
    __table_args__ = {'schema': 'dw'}

    fact_sales_tk = Column(BigInteger, primary_key=True, autoincrement=True) # Primarni ključ

# Strani ključevi prema svim dimenzijama (zvijezda - star schema)
# Ovi ključevi povezuju činjenice s kontekstom (dimenzijama)
    country_tk = Column(BigInteger, ForeignKey('dw.dim_country.country_tk')) # Gdje je prodano
    retailer_tk = Column(BigInteger, ForeignKey('dw.dim_retailer.retailer_tk')) # Tko je prodao
    product_tk = Column(BigInteger, ForeignKey('dw.dim_product.product_tk')) # Što je prodano
    order_method_tk = Column(BigInteger, ForeignKey('dw.dim_order_method.order_method_tk')) # Kako je prodano
    date_tk = Column(Integer, ForeignKey('dw.dim_date.date_tk')) # Kada je prodano
# Mjere (Measures) - numeričke vrijednosti koje analiziramo 
    revenue = Column(Float) # Prihod od prodaje
    quantity = Column(BigInteger) # Količina prodanih proizvoda
    gross_margin = Column(Float) # Bruto marža (profitabilnost)


# Testiranje koncekcije
print("Testiram konekciju...")
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("Konekcija uspješna!")

# Obriši postojeće tablice ako postoje
# Brišemo tablice ako postoje da bi mogli kreirati nove iz početka
# Ovo je korisno tijekom razvoja, ali u produkciji bi se radilo drugačije!
print("\nBrišem postojeće tablice...")
try:
# checkfirst=True prvo provjerava postoji li tablica prije brisanja
# Brišemo u obrnutom redoslijedu od kreiranja zbog stranih ključeva
# (prvo fact tablica koja referencira dimenzije, pa onda dimenzije)
    FactSales.__table__.drop(engine, checkfirst=True)
    DimDate.__table__.drop(engine, checkfirst=True)
    DimOrderMethod.__table__.drop(engine, checkfirst=True)
    DimCountry.__table__.drop(engine, checkfirst=True)
    DimProduct.__table__.drop(engine, checkfirst=True)
    DimRetailer.__table__.drop(engine, checkfirst=True)
    print("Postojeće tablice obrisane")
except:
    print("Nema postojećih tablica")

# Kreiraj dimenzijske tablice
print("\nKreiram dimenzijske tablice...")
Base.metadata.create_all(engine) # Base.metadata.create_all stvara sve tablice definirane ORM klasama
print("Dimenzijske tablice kreirane!")

# POPUNJAVANJE PODACIMA (ETL)
# ETL = Extract (izvlačenje), Transform (transformacija), Load (punjenje)

print("\nPopunjavam dimenzijske tablice podacima...")
# EXTRACT: SQL upit za dohvat podataka iz izvorne baze

# 1. dim_retailer
print("   Popunjavam dim_retailer...")
retailers_query = """
SELECT DISTINCT
    rt.id AS retailer_id,
    rt.name AS name,
    rt.speciality_store AS speciality_store
FROM retailer_type rt
"""
# Pandas read_sql izvršava upit i sprema rezultat u DataFrame
df_retailers = pd.read_sql(retailers_query, engine) # TRANSFORM: Dodavanje SCD Type 2 atributa
df_retailers['version'] = 1 # Prva verzija svakog zapisa
df_retailers['date_from'] = '1900-01-01' # Početak važenja - minimalni datum
df_retailers['date_to'] = '9999-12-31' # Kraj važenja - maksimalni datum (još uvijek aktivno)
# LOAD: Spremanje u dimenzijsku tablicu
# if_exists='append' dodaje nove retke, ne briše postojeće
df_retailers.to_sql('dim_retailer', engine, schema='dw', if_exists='append', index=False)
print(f"      Uneseno {len(df_retailers)} redaka")

# 2. dim_product
print("   Popunjavam dim_product...")
products_query = """
SELECT DISTINCT
    p.id AS product_id,
    p.name AS product_name,
    pt.name AS product_type_name,
    pl.name AS product_line_name
FROM product p
INNER JOIN product_type pt ON p.product_type_fk = pt.id
INNER JOIN product_line pl ON pt.product_line_fk = pl.id
"""
df_products = pd.read_sql(products_query, engine)
df_products['version'] = 1
df_products['date_from'] = '1900-01-01'
df_products['date_to'] = '9999-12-31'
df_products.to_sql('dim_product', engine, schema='dw', if_exists='append', index=False)
print(f"      Uneseno {len(df_products)} redaka")

# 3.  dim_country
print("   Popunjavam dim_country...")
countries_query = """
SELECT DISTINCT
    c.id AS country_id,
    c.name AS name,
    c.population AS population,
    c.region AS region
FROM country c
"""
df_countries = pd.read_sql(countries_query, engine)
df_countries['version'] = 1
df_countries['date_from'] = '1900-01-01'
df_countries['date_to'] = '9999-12-31'
df_countries.to_sql('dim_country', engine, schema='dw', if_exists='append', index=False)
print(f"      Uneseno {len(df_countries)} redaka")

# 4.  dim_order_method
print("   Popunjavam dim_order_method...")
order_methods_query = """
SELECT DISTINCT
    om.id AS order_method_id,
    om.name AS name
FROM order_method om
"""
df_order_methods = pd.read_sql(order_methods_query, engine)
df_order_methods['version'] = 1
df_order_methods['date_from'] = '1900-01-01'
df_order_methods['date_to'] = '9999-12-31'
df_order_methods.to_sql('dim_order_method', engine, schema='dw', if_exists='append', index=False)
print(f"      Uneseno {len(df_order_methods)} redaka")

# 5.  dim_date
print("   Popunjavam dim_date...")
dates_query = """
SELECT DISTINCT
    year,
    quarter
FROM sales
WHERE year IS NOT NULL AND quarter IS NOT NULL
ORDER BY year, quarter
"""
df_dates = pd.read_sql(dates_query, engine)

# Obrada kvartala
# TRANSFORMACIJA: Pretvaranje 'Q1' u broj 1, 'Q2' u 2, itd.
# str.extract(r'(\d+)') izvlači samo broj iz stringa (npr. 'Q1' -> '1')
df_dates['quarter_num'] = df_dates['quarter'].str.extract(r'(\d+)').astype(int)

# Finalni dataframe
df_dates_final = pd.DataFrame({
    'year': df_dates['year'],
    'quarter': df_dates['quarter_num']
})

df_dates_final.to_sql('dim_date', engine, schema='dw', if_exists='append', index=False)
print(f"      Uneseno {len(df_dates_final)} redaka")

# 6. fact_sales
print("   Popunjavam fact_sales...")

# Obriši postojeće podatke
with engine.connect() as conn:
    conn.execute(text("DELETE FROM dw.fact_sales"))
    conn.commit()

# Insert podataka
fact_query = """
INSERT INTO dw.fact_sales (retailer_tk, product_tk, country_tk, order_method_tk, date_tk, revenue, quantity, gross_margin)
SELECT 
    dr.retailer_tk, -- surograt prodavača 
    dp.product_tk, -- surogat proizvoda
    dc.country_tk, -- surogat država
    dom.order_method_tk, -- surogat načina narudžbe
    dd.date_tk, -- surogat datuma
    s.revenue, -- mjera prihoda od prodaje
    s.quantity, -- mjera količine prodanih proizvoda
    s.gross_margin -- mjera bruto marže (profitabilnosti)
FROM sales s
-- Svaki JOIN povezuje izvornu tablicu s dimenzijom preko prirodnog ključa
-- Uvjet date_to = '9999-12-31' osigurava da uzimamo samo trenutno aktivne verzije dimenzija
INNER JOIN dim_retailer dr ON s.retailer_type_fk = dr.retailer_id AND dr.date_to = '9999-12-31'
INNER JOIN dim_product dp ON s.product_fk = dp.product_id AND dp.date_to = '9999-12-31'
INNER JOIN dim_country dc ON s.country_fk = dc.country_id AND dc.date_to = '9999-12-31'
INNER JOIN dim_order_method dom ON s.order_method_fk = dom.order_method_id AND dom.date_to = '9999-12-31'
INNER JOIN dim_date dd ON s.year = dd.year 
    -- Ponovno ekstrakcija broja iz 'Q1', 'Q2' itd. za spajanje s numeričkim kvartalom
    AND CAST(SUBSTRING(s.quarter, 2, 1) AS UNSIGNED) = dd.quarter
"""

with engine.connect() as conn:
    result = conn.execute(text(fact_query))
    conn.commit()
    print(f"      Uneseno {result.rowcount} redaka u fact_sales")

# VERIFIKACIJA

print("\n" + "=" * 50)
print("VERIFIKACIJA DIMENZIJSKOG MODELA")
print("=" * 50)

print("\nBroj redaka po tablicama:")
with engine.connect() as conn:
    tables = ['dim_retailer', 'dim_product', 'dim_country', 'dim_order_method', 'dim_date', 'fact_sales']
    for table in tables:
        result = conn.execute(text(f"SELECT COUNT(*) FROM dw.{table}"))
        count = result.scalar()
        print(f"   {table}: {count:,} redaka")

print("\nUzorak podataka iz fact_sales (prvih 5 redaka):")
preview_query = """
SELECT 
    r.name AS retailer,
    p.product_name,
    c.name AS country,
    om.name AS order_method,
    d.year,
    d.quarter,
    f.revenue,
    f.quantity,
    f.gross_margin
FROM dw.fact_sales f
INNER JOIN dw.dim_retailer r ON f.retailer_tk = r.retailer_tk
INNER JOIN dw.dim_product p ON f.product_tk = p.product_tk
INNER JOIN dw.dim_country c ON f.country_tk = c.country_tk
INNER JOIN dw.dim_order_method om ON f.order_method_tk = om.order_method_tk
INNER JOIN dw.dim_date d ON f.date_tk = d.date_tk
LIMIT 5
"""

df_preview = pd.read_sql(preview_query, engine)
print(df_preview.to_string(index=False))

print("\n" + "=" * 50)
print("DIMENZIJSKI MODEL USPJEŠNO KREIRAN I POPUNJEN!")
print("=" * 50)