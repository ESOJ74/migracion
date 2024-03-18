import glob

import pandas as pd

from commons.SQLConnections import get_df

def query_assets_definition(path):
    query = f'''SELECT path, plant_id, type, latitude, longitude, description
                FROM am.assets_definition
                WHERE path='{path}' '''
    return get_df(query)

df_bbdd = query_assets_definition('GOO')
print(df_bbdd)

def read_excel_data(path):
    with pd.ExcelFile(glob.glob(f'datosplantas/GOO/*.xlsx')[0]) as xlsx_file:
        df_parents_child = xlsx_file.parse(sheet_name='Parent-Child links')
        df_elements_all = xlsx_file.parse(sheet_name='Elements_ALL')[
            ['Device Code', 'Device Type3']]
    return df_parents_child, df_elements_all

df_parents_child, df_elements_all = read_excel_data('GOO')
print(df_elements_all)
print(df_parents_child)

merged_df = (df_parents_child
                 .merge(df_elements_all,
                        left_on='Child Device',
                          right_on='Device Code'))
print(merged_df)