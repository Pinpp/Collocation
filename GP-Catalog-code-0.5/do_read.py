# -*- coding: utf-8 -*-

from func_tools import load_params, con_db, close_db, sql_act

# db = con_db()
db = None

def read_target_obs_params_func(obj_id, obs_para_name):
    ''''''
    sql = "select a.default_val, b.opc_value from observation_parameters_conf a, target_observation_parameters_rec b where a.opc_id=b.opc_id and a.status_flag=1 and b.status_flag=1 and a.obs_ser_id=1 and a.obs_para_name='{}' and b.obj_id={}".format(
        obs_para_name, obj_id)
    # print(sql)
    res = sql_act(db, sql)
    if res:
        return res[0]


def read_target_obs_params(obj_id):
    '''
        return config:
            {
                'ECL_CONF': xxx,
                'GRM_CONF': xxx,
                'VT_CONF': {
                    'EXPOSURE_TIME': xxx
                },
                'PF_CONF': {
                    'STABILITY': xxx
                }
            }
    '''
    config = {}

    # ECL_CONF
    vals = read_target_obs_params_func(obj_id, 'ECL_CONF')
    if vals:
        config['ECL_CONF'] = vals[0]
    else:
        config['ECL_CONF'] = 'NOT_USED'
    
    # GRM_CONF
    vals = read_target_obs_params_func(obj_id, 'GRM_CONF')
    if vals:
        config['GRM_CONF'] = vals[0]
    else:
        config['GRM_CONF'] = 'NOT_USED'

    # MXT_CONF
    vals = read_target_obs_params_func(obj_id, 'MXT_CONF')
    if vals:
        config['MXT_CONF'] = vals[0]
    else:
        config['MXT_CONF'] = 'NOT_USED'

    # VT_CONF
    config['VT_CONF'] = {}
    ### EXPOSURE_TIME
    vals = read_target_obs_params_func(obj_id, 'EXPOSURE_TIME')
    if vals:
        config['VT_CONF']['EXPOSURE_TIME'] = vals[1]
    else:
        config['VT_CONF']['EXPOSURE_TIME'] = ''
    ### WINDOW_SIZE
    vals = read_target_obs_params_func(obj_id, 'WINDOW_SIZE')
    if vals:
        config['VT_CONF']['WINDOW_SIZE'] = vals[1]
    else:
        config['VT_CONF']['WINDOW_SIZE'] = ''
    ### INTERVAL_BETWEEN_IMG
    vals = read_target_obs_params_func(obj_id, 'INTERVAL_BETWEEN_IMG')
    if vals:
        config['VT_CONF']['INTERVAL_BETWEEN_IMG'] = vals[1]
    else:
        config['VT_CONF']['INTERVAL_BETWEEN_IMG'] = ''
    ### READ_SPEED
    vals = read_target_obs_params_func(obj_id, 'READ_SPEED')
    if vals:
        config['VT_CONF']['READ_SPEED'] = vals[0]
    else:
        config['VT_CONF']['READ_SPEED'] = ''
    ### READ_CHANNEL
    vals = read_target_obs_params_func(obj_id, 'READ_CHANNEL')
    if vals:
        config['VT_CONF']['READ_CHANNEL'] = vals[0]
    else:
        config['VT_CONF']['READ_CHANNEL'] = ''
    ### CLEANING
    vals = read_target_obs_params_func(obj_id, 'CLEANING')
    if vals:
        config['VT_CONF']['CLEANING'] = vals[0]
    else:
        config['VT_CONF']['CLEANING'] = ''

    # PF_CONF
    config['PF_CONF'] = {}
    ### STABILITY
    vals = read_target_obs_params_func(obj_id, 'STABILITY')
    if vals:
        config['PF_CONF']['STABILITY'] = vals[0]
    else:
        config['PF_CONF']['STABILITY'] = ''
    ### MOON_CHECK
    vals = read_target_obs_params_func(obj_id, 'MOON_CHECK')
    if vals:
        config['PF_CONF']['MOON_CHECK'] = vals[0]
    else:
        config['PF_CONF']['MOON_CHECK'] = ''

    return config


def read_target(obj_id):
    ''''''
    data_line = []
    sql = "select svomdb_id from gp_catalog_targets where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        data_line.append(res[0][0])
    else:
        data_line.append('')
    sql = "select obj_source, obs_type, radeg, decdeg, req_obs_duration_in_minutes, min_cont_obs_duration_in_minutes, req_completeness, pointing_attribute, priority from target where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        for i in res[0]:
            if i:
                data_line.append(i)
            else:
                data_line.append('')
    else:
        for i in range(9):
            data_line.append('')
    sql = "select suggested_obs_t_beg, suggested_obs_t_end from target_time_allocation where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        for i in res[0]:
            data_line.append(i+'Z') ### format
    else:
        for i in range(2):
            data_line.append('')
    sql = "select combined_observation from target_combined_observation_rec where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        comb_tel = []
        for i in res:
            # print('res',i)
            comb_tel.append(i[0])
        data_line.append(','.join(comb_tel))
    else:
        data_line.append('')
    sql = "select user_group from gp_catalog_targets where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        data_line.append(res[0][0])
    else:
        data_line.append('')

    # data_line: svomdb_id, obj_source, obs_type, rad, decd, req_obs_duration_in_minutes, min_cont_obs_duration_in_minutes, req_completeness, pointing_attribute, priority, suggested_obs_t_beg, suggested_obs_t_end, combined_observation, user_group
    return data_line

def read_catalog(catalog_id):
    ''''''
    sql = "select window_begin, window_end from gp_catalog where gp_catalog_id={}".format(catalog_id)
    res = sql_act(db, sql)
    if res:
        return res[0]
    else:
        return ['','']

def read_main(catalog_id):
    global db
    db = con_db()

    data_lines = []
    catalog_info = read_catalog(catalog_id)
    # catalog_info: window_begin, window_end
    sql = "select obj_id from gp_catalog_targets where gp_catalog_id={}".format(catalog_id)
    res = sql_act(db, sql)
    # print(res)
    if res:
        for obj_id in res:
            obj_id = obj_id[0]
            obj_info = read_target(obj_id)
            # obj_info: svomdb_id, obj_source, obs_type, rad, decd, req_obs_duration_in_minutes, min_cont_obs_duration_in_minutes, req_completeness, pointing_attribute, priority, suggested_obs_t_beg, suggested_obs_t_end, combined_observation, user_group
            inst_info = read_target_obs_params(obj_id)
            # {
            #     'ECL_CONF': xxx,
            #     'GRM_CONF': xxx,
            #     'VT_CONF': {
            #         'EXPOSURE_TIME': xxx
            #     },
            #     'PF_CONF': {
            #         'STABILITY': xxx
            #     }
            # }
            # print(inst_info)
            data_line = []
            data_line.extend(catalog_info)
            data_line.extend(obj_info)
            data_line.append(inst_info['ECL_CONF'])
            data_line.append(inst_info['GRM_CONF'])
            data_line.append(inst_info['MXT_CONF'])
            # VT_CONF_keys: EXPOSURE_TIME WINDOW_SIZE INTERVAL_BETWEEN_IMG READ_SPEED READ_CHANNEL CLEANING
            # PF_CONF_keys: STABILITY MOON_CHECK
            data_line.append(inst_info['VT_CONF']['EXPOSURE_TIME'])
            data_line.append(inst_info['VT_CONF']['WINDOW_SIZE'])
            data_line.append(inst_info['VT_CONF']['INTERVAL_BETWEEN_IMG'])
            data_line.append(inst_info['VT_CONF']['READ_SPEED'])
            data_line.append(inst_info['VT_CONF']['READ_CHANNEL'])
            data_line.append(inst_info['VT_CONF']['CLEANING'])
            data_line.append(inst_info['PF_CONF']['STABILITY'])
            data_line.append(inst_info['PF_CONF']['MOON_CHECK'])
            data_lines.append(data_line)
            # data_line: 
            #   window_begin-0, window_end-1, svomdb_id-2, obj_source-3, obs_type-4, 
            #   rad-5, decd-6, req_obs_duration_in_minutes-7, min_cont_obs_duration_in_minutes-8, 
            #   req_completeness-9, pointing_attribute-10, priority-11, suggested_obs_t_beg-12, suggested_obs_t_end-13, 
            #   combined_observation-14, user_group-15, ECL_CONF-16, GRM_CONF-17, MXT_CONF-18, VT_EXPOSURE_TIME-19, VT_WINDOW_SIZE-20,
            #   VT_INTERVAL_BETWEEN_IMG-21, VT_READ_SPEED-22, VT_READ_CHANNEL-23, VT_CLEANING-24, STABILITY-25, MOON_CHECK-26
    
    close_db(db)

    return data_lines
            
if __name__ == '__main__':
    catalog_id = 119
    data_lins_ouput = read_main(catalog_id)
    print(data_lins_ouput)