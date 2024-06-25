from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim 
import pandas as pd
import numpy as np
import os
import time
import logging

""" def pegar_cep(df_chunk):
    nom = Nominatim(user_agent="meu-aplicativo")
    latitudes = []
    longitudes = []

    # Iterando pelos CEPs e buscando as coordenadas
    for cep in df_chunk["customer_zip_code_prefix"]:
        x = nom.geocode(str(cep))
        
        if x is not None:
            latitudes.append(x.latitude)
            longitudes.append(x.longitude)
        else:
            latitudes.append(None)
            longitudes.append(None)

    # Atribuindo as listas de latitudes e longitudes ao DataFrame de uma vez
    df_chunk["Customer Latitude"] = latitudes
    df_chunk["Customer Longitude"] = longitudes
    
    return df_chunk

def processar_cep_concurrente(df, num_threads=6):
    # Divide o DataFrame em partes iguais para cada thread
    df_chunks = np.array_split(df, num_threads)

    results = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_chunk = {executor.submit(pegar_cep, chunk): chunk for chunk in df_chunks}
        
        for future in as_completed(future_to_chunk):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing chunk: {e}")

    # Concatenando todos os resultados de volta em um único DataFrame
    df_final = pd.concat(results)
    df_final.reset_index(inplace=True, drop=True)

    return df_final """



def pegar_cep(df_chunk):
    nom = Nominatim(user_agent="meu-aplicativo", timeout=10)
    latitudes = []
    longitudes = []

    for cep in df_chunk["customer_zip_code_prefix"]:
        retries = 3
        while retries > 0:
            try:
                x = nom.geocode(str(cep))
                if x is not None:
                    latitudes.append(x.latitude)
                    longitudes.append(x.longitude)
                else:
                    latitudes.append(None)
                    longitudes.append(None)
                break
            except Exception as e:
                retries -= 1
                logging.warning(f"Error geocoding {cep}: {e}. Retries left: {retries}")
                time.sleep(1)  # Espera 1 segundo antes de tentar novamente
        else:
            latitudes.append(None)
            longitudes.append(None)

    df_chunk["Customer Latitude"] = latitudes
    df_chunk["Customer Longitude"] = longitudes
    
    return df_chunk

def processar_cep_concurrente(df, num_threads=6):
    df_chunks = np.array_split(df, num_threads)

    results = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_chunk = {executor.submit(pegar_cep, chunk): chunk for chunk in df_chunks}
        
        for future in as_completed(future_to_chunk):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Error processing chunk: {e}")

    if results:
        df_final = pd.concat(results)
        df_final.reset_index(inplace=True, drop=True)
        return df_final
    else:
        raise ValueError("No objects to concatenate")

df = pd.read_csv("Data/Dataframelimpa_sem_latlong.csv")
df_final = processar_cep_concurrente(df, num_threads=6)


# Definindo o caminho para a pasta Data
current_dir = os.path.dirname(os.path.abspath('__file__'))
data_dir = os.path.join(current_dir, 'Data')


# Caminho completo para o arquivo CSV
csv_path = os.path.join(data_dir, 'Dataframelimpa_com_latlong.csv')


# Exportando o DataFrame para um arquivo CSV
df_final.to_csv(csv_path, index=False)