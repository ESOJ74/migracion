import glob

import pandas as pd

from commons.SQLConnections import get_df


def query_assets_definition(path):
    query = f'''SELECT path, plant_id, type, latitude, longitude, description
                FROM am.assets_definition
                WHERE path='{path}' '''
    return get_df(query)


def read_excel_data(path):
    with pd.ExcelFile(glob.glob('datosplantas/SS/*.xlsx')[0]) as xlsx_file:
        df_parents_child = xlsx_file.parse(sheet_name='Simple Model')
        df_elements_all = xlsx_file.parse(sheet_name='SS678.Bluence_devices')[
            ['Device Code', 'Device Type']]
        df_parents = xlsx_file.parse(sheet_name='Complex Model')
       
        df_parents = df_parents[df_parents['PLANT'] == 'SS8']
        df_parents = df_parents[df_parents['Device Type'].isin(['Tracker', 'Combiner_Box_L1', 'Inverter', 
                                             'Master_Tracker'])][['Parent \nDevice', 'Child \nDevice']]
        df_parents_child = df_parents_child[df_parents_child['PLANT'] == 'SS8']
        df_parents_child = df_parents_child[df_parents_child['Device Type'].isin(['Tracker', 'Combiner_Box_L1', 'Inverter', 
                                             'Master_Tracker'])]
        df_elements_all = df_elements_all[df_elements_all['Device Type'].isin(['Tracker', 'Combiner_Box_L1', 'Inverter', 
                                             'Master_Tracker'])][['Device Type', 'Device Code']]
    
    return df_parents_child, df_elements_all, df_parents          


def merge_dataframes(df, df_parents_child, df_elements_all, df_parents):
    df_elements_all['Device Code'] = df_elements_all['Device Code'].fillna('')
    merged_df = (df_parents_child
                 .merge(df_elements_all, left_on='Device_ID', right_on='Device Code')
                 .merge(df_parents, left_on='Device_ID', right_on='Child \nDevice')
                 .assign(
                     plant_id=df['plant_id'][0],
                     latitude=df['latitude'][0],
                     longitude=df['longitude'][0],
                     manufacturer='',
                     model='',
                     description=''
                 )
                 .rename(columns={'Device Type_x': 'type', 'Device\nDC Power\n(kWp)': 'downstream_power',
                                  'Device_ID': 'path', 'Parent \nDevice': 'parent_path',
                                  'Group': 'group_asset'})
                 [['path', 'parent_path', 'plant_id', 'type', 'latitude', 'longitude',
                   'manufacturer', 'model', 'description', 'downstream_power', 'group_asset']])
    return merged_df[merged_df['type'].isin(['Tracker', 'Combiner_Box_L1', 'Inverter', 
                                             'Master_Tracker'])].drop_duplicates()


def generar_csv_assets_definition(paths):
    for path in paths:
        df_bbdd = query_assets_definition(path)
        print(df_bbdd)
        
        if len(df_bbdd) > 0:
            df_parents_child, df_elements_all, df_parents = read_excel_data(path)
            print(df_parents_child[df_parents_child['PLANT'] == 'SS8'])
            print(df_elements_all)            
            df_result = merge_dataframes(df_bbdd, df_parents_child[df_parents_child['PLANT'] == 'SS8'], df_elements_all, df_parents)
            (df_result
             .assign(
                 path='SS8' + '.' + df_result['path'],
                 parent_path='SS8' + '.' + df_result['parent_path']
                 )
             .to_csv("SS8.csv", index=False))
        else:
            print("No hay datos en BBDD")

#generar_csv_assets_definition(['SS8'])


def eliminar_duplicados():
    (pd.read_csv('SS8.csv')
       .drop_duplicates(subset=['path'], keep='first')
       .fillna('')
       .to_csv('SS8_sin_duplicados.csv', index=False)
     )
#eliminar_duplicados()


def generar_csv_electrical_relations():
    path = 'SS6'

    query = '''select id, path, parent_path
               from am.assets_definition where plant_id='5' order by id'''
    df_bbdd = get_df(query)

    """df_bbdd = pd.read_csv(f'{path}.csv')[['path', 'parent_path']]
    df_bbdd['id'] = df_bbdd.index"""

    (df_bbdd
     .merge(df_bbdd, left_on='parent_path', right_on='path',
            suffixes=('', '_parent'))[['id', 'id_parent']]
     .drop_duplicates().sort_values(by=['id', 'id_parent'])
     .rename(columns={'id': 'child_id', 'id_parent': 'parent_id'})
     .to_csv(f'SS6_assets_electrical.csv', index=False)
     )


generar_csv_electrical_relations()
