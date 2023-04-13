"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import numpy as np
import re

def ingest_data():
    def agregar_separador(val):
        return val[:7] + '$' + val[7:19] + '$' + val[19:36] + '$' + val[36:]
    df = pd.read_csv('clusters_report.txt', delimiter='\t', skiprows=4, header=None, engine='python')
    df = df.applymap(agregar_separador)
    df = df[0].str.split('$', expand=True)
    df["cambio_grupo"] = (df[0] != df[0].shift()) & (df[0] != "")
    df["grupo"] = df["cambio_grupo"].cumsum()
    df["grupo_par"] = ((df["grupo"] - 1) // 2) + 1
    df_concat = df.groupby("grupo_par").agg({
        0: "first",
        1: "first",
        2: "first",
        3: lambda x: "\n".join(x)
        }).reset_index()
    del df_concat["grupo_par"]
    df_concat[[0, 1, 2]] = df_concat[[0, 1, 2]].replace({"%":"","," :"."}, regex=True)
    df_concat[3] = df_concat[3].str.replace(r'\.$', '', regex=True)
    regex = re.compile(r'\s{2,}')
    def eliminar_espacios(cell):
        return regex.sub(' ', cell).strip()
    df_concat = df_concat.applymap(eliminar_espacios)
    df_concat = df_concat.rename(columns={0: "cluster", 1: "cantidad_de_palabras_clave", 2: "porcentaje_de_palabras_clave", 3: "principales_palabras_clave"})
    df_concat["cluster"] = df_concat["cluster"].astype(int)
    df_concat["cantidad_de_palabras_clave"] = df_concat["cantidad_de_palabras_clave"].astype(int)
    df_concat["porcentaje_de_palabras_clave"] = df_concat["porcentaje_de_palabras_clave"].astype(float)

    return df_concat