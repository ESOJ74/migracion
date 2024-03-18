import glob

import pandas as pd

from commons.SQLConnections import get_df
import numpy as np

def query_assets_definition(device_type):
    query = f'''select id, path
               from am.assets_definition where plant_id='12' and type = '{device_type}' order by id'''
    return get_df(query)



def read_excel_data():
    with pd.ExcelFile(glob.glob('datosplantas/GOO/*.xlsx')[0]) as xlsx_file:
        df_parents_child = xlsx_file.parse(sheet_name='GOYM_ISOTROL_GOO')[
            ['Address (nombre interno)', 'Device Type2', 'Device Code']]
    return df_parents_child

# TODO Rename this here and in `inverters`
def _extracted_from_inverters_6(df, arg1):
    result = pd.DataFrame()
    result['asset_id'] = list(df['id'].values)
    result['signal_name'] = arg1    
    return result

def inverters():
    df = query_assets_definition('Inverter')
    df['provider'] = 'isotrol'    

    df_2 = _extracted_from_inverters_6(df, 'Act_Energy_D')
    df_3 = _extracted_from_inverters_6(df, 'Setpoint')
    df_4 = pd.concat([df_2, df_3]).sort_values(by='asset_id')    

    df_5 = df.merge(df_4, left_on='id', right_on='asset_id')[['asset_id', 'provider', 'signal_name', 'path']]

    df_6 = read_excel_data()
    df_6 = df_6[df_6['Device Type']=='Inverter'].assign(
                    Device='MOR' + '.' + df_6['Device Code']
                    )

    df_7 = df_5.merge(df_6, left_on='path', right_on='Device')

    df_7 = df_7.assign(
        provider_signal_path = np.where(
            df_7['signal_name']=='Act_Energy_D', df_7['Address (nombre interno)'] + '.Act_Energy_D_5M', df_7['Address (nombre interno)'] + '.Setpoint_Pac_5M')
    )[['asset_id', 'provider', 'signal_name', 'provider_signal_path']]
    df_7.to_csv('MOR.signal_relations.csv', index=False)
    print(df_7)

#inverters()
    
def trackers():
    df = query_assets_definition('Tracker')
    df['provider'] = 'isotrol'    
    
    df_2 = _extracted_from_inverters_6(df, 'Trk_Inclin')
    df_3 = _extracted_from_inverters_6(df, 'Trk_Inclin_SetPoint')
    df_4 = pd.concat([df_2, df_3]).sort_values(by='asset_id')    

    df_5 = df.merge(df_4, left_on='id', right_on='asset_id')[['asset_id', 'provider', 'signal_name', 'path']]
    print(df_5)
    df_6 = read_excel_data()
    df_6 = df_6[df_6['Device Type2']=='Tracker'].assign(
                    Device='GOO' + '.' + df_6['Device Code']
                    )
    print(df_6)
    df_7 = df_5.merge(df_6, left_on='path', right_on='Device')
    print(df_7)
    df_7 = df_7.assign(
        provider_signal_path = np.where(
            df_7['signal_name']=='Trk_Inclin', df_7['Address (nombre interno)'] + '.Trk_Inclin_5M', df_7['Address (nombre interno)'] + '.Trk_Inclin_SetPoint_5M')
    )[['asset_id', 'provider', 'signal_name', 'provider_signal_path']]
    df_7.to_csv('GOO.signal_relations_trackers.csv', index=False)
    print(df_7)

#trackers()


