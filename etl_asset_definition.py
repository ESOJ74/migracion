import glob

import pandas as pd

from commons.SQLConnections import get_df


def query_assets_definition(path):
    query = f'''SELECT path, plant_id, type, latitude, longitude, description
                FROM am.assets_definition
                WHERE path='{path}' '''
    return get_df(query)


def read_excel_data(path):
    with pd.ExcelFile(glob.glob('datosplantas/MOR/*.xlsx')[0]) as xlsx_file:
        df_parents_child = xlsx_file.parse(sheet_name='Parent-Child links_MOR')
        df_elements_all = xlsx_file.parse(sheet_name='Elements_ALL')[
            ['Device Code', 'Device Type']]
    return df_parents_child, df_elements_all


def merge_dataframes(df, df_parents_child, df_elements_all):
    df_elements_all['Device Code'] = df_elements_all['Device Code'].fillna('')
    merged_df = (df_parents_child
                 .merge(df_elements_all, left_on='Child Device', right_on='Device Code')
                 .assign(
                     plant_id=df['plant_id'][0],
                     latitude=df['latitude'][0],
                     longitude=df['longitude'][0],
                     manufacturer='',
                     model='',
                     description=''
                 )
                 .rename(columns={'Device Type': 'type', 'DC Cap. (kWp).1': 'downstream_power',
                                  'Child Device': 'path', 'Parent Device': 'parent_path'})
                 [['path', 'parent_path', 'plant_id', 'type', 'latitude', 'longitude',
                   'manufacturer', 'model', 'description', 'downstream_power']])
    return merged_df


def generar_csv_assets_definition(paths):
    for path in paths:
        df_bbdd = query_assets_definition(path)
        if len(df_bbdd) > 0:
            df_parents_child, df_elements_all = read_excel_data(path)
            df_result = merge_dataframes(df_bbdd, df_parents_child, df_elements_all)
            (df_result
             .assign(
                 parent_path='MOR' + '.' + df_result['parent_path'],
                 path='MOR' + '.' + df_result['path']
                 )
             .to_csv(f"MOR.csv", index=False))          
        else:
            print("No hay datos en BBDD")

#generar_csv_assets_definition(['MOR'])


def eliminar_duplicados():
    (pd.read_csv('MOR.csv')
       .drop_duplicates(subset=['path'], keep='first')
       .fillna('')
       .to_csv('MOR_sin_duplicados.csv', index=False)
     )
#eliminar_duplicados()


def generar_csv_electrical_relations():
    path = 'LSF'

    query = '''select id, path, parent_path
               from am.assets_definition where plant_id='9' order by id'''
    df_bbdd = get_df(query)

    """df_bbdd = pd.read_csv(f'{path}.csv')[['path', 'parent_path']]
    df_bbdd['id'] = df_bbdd.index"""

    (df_bbdd
     .merge(df_bbdd, left_on='parent_path', right_on='path',
            suffixes=('', '_parent'))[['id', 'id_parent']]
     .drop_duplicates().sort_values(by=['id', 'id_parent'])
     .rename(columns={'id': 'child_id', 'id_parent': 'parent_id'})
     .to_csv(f'assets_electrical_LSF.csv', index=False)
     )


generar_csv_electrical_relations()
