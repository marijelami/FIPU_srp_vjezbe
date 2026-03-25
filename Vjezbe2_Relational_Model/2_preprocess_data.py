import pandas as pd

"""
2. Skripta za predprocesiranje skupa podataka

Predprocesiranje ovisi o skupu podataka. Potrebno je prilagoditi skriptu za vlastiti skup.

Checkpoint 1 je obuhvaćao pronalazak skupa podataka i analizu. Ovdje je potrebno napraviti predprocesiranje na temelju analize.

U nastavku je prikazan primjer predprocesiranja skupa podataka naš case Oprema d.d.
"""

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "data/WA_Sales_Products_2012-14.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

df['Retailer country'] = df['Retailer country'].replace('USA', 'United States') # Zamjena vrijednosti zbog API-a koji pozivamo kasnije u drugom koraku
df['Retailer country'] = df['Retailer country'].replace('UK', 'United Kingdom') # Zamjena vrijednosti zbog API-a koji pozivamo kasnije u drugom koraku
df = df.dropna() # Brisanje redaka s nedostajućim vrijednostima
df.columns = df.columns.str.lower() # Pretvori sve nazive stupaca u mala slova
df.columns = df.columns.str.replace(' ', '_') # Zamjena razmaka u nazivima stupaca s donjom crtom
print("CSV size after: ", df.shape) # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head()) # Ispis prvih redaka dataframe-a

# Count if there are duplicates
duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}") # Ispis broja duplikata

# Random dijeljenje skupa podataka na dva dijela 80:20 (trebat će nam kasnije)
df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

# Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df.to_csv("2_relational_model/processed/WA_Sales_Products_2012-14_PROCESSED.csv", index=False) # Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df20.to_csv("2_relational_model/processed/WA_Sales_Products_2012-14_PROCESSED_20.csv", index=False) # Spremanje 20% skupa podataka u novu CSV datoteku