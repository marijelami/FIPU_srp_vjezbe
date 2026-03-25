# Imports
import pandas as pd
import json
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, insert
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import List, Dict, Any

'''
3. Skripta za stvaranje sheme i import podataka u bazu podataka


Potrebno je modificirati skriptu za vlastiti skup podataka.

U ovom koraku stvaramo shemu baze podataka i importiramo podatke u bazu podataka.
U nastavku je prikazan primjer stvaranja sheme i importa podataka našeg case Oprema d.d.
'''

# Putanja do predprocesirane CSV datoteke
CSV_FILE_PATH = "2_relational_model/processed/WA_Sales_Products_2012-14_PROCESSED.csv"

# Učitavanje CSV datoteke u dataframe
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")  # Print dataset size
print(df.head())  # Preview first few rows

# Database Connection
Base = declarative_base()

# Definiranje sheme baze podataka
# --------------------------------------------------------------
class Country(Base):
    __tablename__ = 'country'
    id = Column(Integer, primary_key=True)
    name = Column(String(45), nullable=False, unique=True)
    population = Column(Integer, nullable=False)
    region = Column(String(45), nullable=False)

class OrderMethod(Base):
    __tablename__ = 'order_method'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class RetailerType(Base):
    __tablename__ = 'retailer_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    speciality_store = Column(Integer, nullable=False)

class ProductLine(Base):
    __tablename__ = 'product_line'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)

class ProductType(Base):
    __tablename__ = 'product_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)
    product_line_fk = Column(Integer, ForeignKey('product_line.id'))

class Product(Base):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45))
    product_type_fk = Column(Integer, ForeignKey('product_type.id'))

class Sales(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    quarter = Column(String(45), nullable=False)
    revenue = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    gross_margin = Column(Float, nullable=False)
    country_fk = Column(Integer, ForeignKey('country.id'))
    order_method_fk = Column(Integer, ForeignKey('order_method.id'))
    product_fk = Column(Integer, ForeignKey('product.id'))
    retailer_type_fk = Column(Integer, ForeignKey('retailer_type.id'))

# Database Connection
engine = create_engine('mysql+pymysql://root:1234@localhost:3306/sales_data', echo=False)
print("="*50)
print("KORAK 1: Spajam se na bazu...")
engine = create_engine('mysql+pymysql://root:1234@localhost:3306/dw', echo=False)

print("KORAK 2: Brišem postojeće tablice...")
Base.metadata.drop_all(engine)

print("KORAK 3: Kreiram nove tablice...")
Base.metadata.create_all(engine)

print("KORAK 4: Tablice kreirane, provjera...")
from sqlalchemy import inspect
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"Tablice u bazi NAKON kreiranja: {tables}")
print("="*50)

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Otvori novu sesiju

# --------------------------------------------------------------
# Import podataka
# --------------------------------------------------------------

# **1. Umetanje zemalja**
countries = df[['retailer_country']].drop_duplicates().rename(columns={'retailer_country': 'name'}) # Dohvatimo jedinstvene zemlje
countries_list = []

# API call za dohvat populacije i regije zemalja
for idx, (i, row) in enumerate(countries.iterrows(), 1):
    response = requests.get(f"https://restcountries.com/v3.1/name/{row['name']}?fullText=true")
    data = json.loads(response.content)
    
    if data:
        country_entry = {
            "id": idx,
            "name": row['name'],
            "population": data[0].get('population', 0),
            "region": data[0].get('region', 'Unknown')
        }
        countries_list.append(country_entry)

session.execute(insert(Country), countries_list) # Bulk insert
session.commit() 

country_map = {c.name: c.id for c in session.query(Country).all()} # Stvori mapiranje zemalja koje će nam trebati kasnije za strane ključeve
# Vrijednost mapiranja: {Country Name: Country ID}
#   {'Canada': 1, 'Mexico': 46, 'Brazil': 344, 'Japan': 600, 'Singapore': 1004, 'Poland': 1277, 'China': 1546, 'Australia': 1851, 'Netherlands': 2131, 'Sweden': 2479, 'Denmark': 2680, 'Finland': 2854, 'France': 3129, 'Germany': 3638, 'Switzerland': 4055, 'United Kingdom': 4373, 'Belgium': 4778, 'Austria': 5041, 'Italy': 5316, 'Spain': 5623, 'United States': 5885}


# **2. Umetanje načina narudžbe**
order_methods = df[['order_method_type']].drop_duplicates().rename(columns={'order_method_type': 'name'}) # Dohvatimo jedinstvene načine narudžbe
order_methods_list = [{str(k): v for k, v in row.items()} for row in order_methods.to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima

session.execute(insert(OrderMethod), order_methods_list)
session.commit()

order_method_map = {om.name: om.id for om in session.query(OrderMethod).all()} # Stvori mapiranje načina narudžbe koje će nam trebati kasnije za strane ključeve


# **3. Umetanje tipa prodavača**
retailer_types = df[['retailer_type']].drop_duplicates().rename(columns={'retailer_type': 'name'}) # Dohvatimo jedinstvene tipove prodavača
retailer_types_list = [{str(k): v for k, v in row.items()} for row in retailer_types.to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima

# Sintetičko generiranje specijaliziranih trgovina
speciality_flags = [0] * (len(retailer_types_list) - 3) + [1] * 3 # Postavi 3 specijalizirane trgovine
random.shuffle(speciality_flags) # Pomiješaj redoslijed specijaliziranih trgovina

# Dodaj specijalizirane trgovine u listu tipova prodavača
for i, entry in enumerate(retailer_types_list):
    entry["speciality_store"] = speciality_flags[i]

session.execute(insert(RetailerType), retailer_types_list) # Bulk insert
session.commit()

retailer_type_map = {rt.name: rt.id for rt in session.query(RetailerType).all()} # Stvori mapiranje tipova prodavača koje će nam trebati kasnije za strane ključeve


# **4. Umetanje linija proizvoda**
product_lines = df[['product_line']].drop_duplicates().rename(columns={'product_line': 'name'}) # Dohvatimo jedinstvene linije proizvoda
product_lines_list = [{str(k): v for k, v in row.items()} for row in product_lines.to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima
session.execute(insert(ProductLine), product_lines_list) # Bulk insert
session.commit()

product_line_map = {pl.name: pl.id for pl in session.query(ProductLine).all()} # Stvori mapiranje linija proizvoda koje će nam trebati kasnije za strane ključeve


# **5. Umetanje tipova proizvoda**
product_types = df[['product_type', 'product_line']].drop_duplicates() # Dohvatimo jedinstvene tipove proizvoda i linije proizvoda
product_types['product_line_fk'] = product_types['product_line'].map(product_line_map) # Mapiraj linije proizvoda iz teksta u ID
product_types = product_types.rename(columns={'product_type': 'name'}).drop(columns=['product_line']) # Preimenuj stupce i izbaci nepotrebne stupce
product_types_list = [{str(k): v for k, v in row.items()} for row in product_types.to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima
session.execute(insert(ProductType), product_types_list) # Bulk insert
session.commit()

product_type_map = {pt.name: pt.id for pt in session.query(ProductType).all()} # Stvori mapiranje tipova proizvoda koje će nam trebati kasnije za strane ključeve


# **6. Umetanje proizvoda**
products = df[['product', 'product_type']].drop_duplicates() # Dohvatimo jedinstvene proizvode i tipove proizvoda
products['product_type_fk'] = products['product_type'].map(product_type_map) # Mapiraj tipove proizvoda iz teksta u ID
products = products.rename(columns={'product': 'name'}).drop(columns=['product_type']) # Preimenuj stupce i izbaci nepotrebne stupce
products_list = [{str(k): v for k, v in row.items()} for row in products.to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima
session.execute(insert(Product), products_list) # Bulk insert
session.commit()

product_map = {p.name: p.id for p in session.query(Product).all()} # Stvori mapiranje proizvoda koje će nam trebati kasnije za strane ključeve


# **7. Insert Sales Data**
sales_data = df[['year', 'quarter', 'revenue', 'quantity', 'gross_margin', 
                 'retailer_country', 'order_method_type', 'product', 'retailer_type']].copy() # Dohvati potrebne stupce

sales_data['country_fk'] = sales_data['retailer_country'].map(country_map) # Mapiraj zemlje iz teksta u ID
sales_data['order_method_fk'] = sales_data['order_method_type'].map(order_method_map) # Mapiraj načine narudžbe iz teksta u ID
sales_data['product_fk'] = sales_data['product'].map(product_map) # Mapiraj proizvode iz teksta u ID
sales_data['retailer_type_fk'] = sales_data['retailer_type'].map(retailer_type_map) # Mapiraj tipove prodavača iz teksta u ID

sales_list = [{str(k): v for k, v in row.items()} for row in sales_data.drop(columns=['retailer_country', 'order_method_type', 'product', 'retailer_type']).to_dict(orient="records")] # Pretvori u listu rječnika sa string ključevima
session.execute(insert(Sales), sales_list) # Bulk insert
session.commit()

print("Data imported successfully!")
