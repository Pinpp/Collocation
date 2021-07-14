# -*- coding: utf-8 -*-

from func_tools import load_params, con_db, close_db, sql_act

# db = con_db()
db = None


def write_gp_catalog(time_beg, time_end):
    '''
        gp_catalog_id: int8
        time_beg: \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ
        time_end: \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ
    '''
    sql = "insert into gp_catalog (window_begin, window_end) values('{}', '{}');".format(
        time_beg, time_end)
    sql_act(db, sql, n=0)
    sql = 'select gp_catalog_id from gp_catalog order by gp_catalog_id desc limit 1;'
    res = sql_act(db, sql)
    if res:
        return res[0][0]


def write_gp_catalog_targets(gp_catalog_id, obj_id, svomdb_id, user_group):
    '''
        gp_catalog_id: int8
        obj_id: int8
        user_group: string
        ***
        svomdb_id: string gp_{obj_id}
    '''
    # svomdb_id = 'gp' + str(obj_id)
    # print(svomdb_id)
    sql = "insert into gp_catalog_targets (gp_catalog_id, obj_id, svomdb_id, user_group) values ({}, {}, '{}', '{}');".format(
        gp_catalog_id, obj_id, svomdb_id, user_group)
    sql_act(db, sql, n=0)


def write_target(obs_type, rad, decd, req_obs_duration, attribute, priority_level, obj_name, obj_source, source_ext_id):
    '''
        req_obs_duration: unit need mins; but input secs
    '''
    attribute_val = 'NULL'
    if attribute and attribute != 'nan':
        attribute_val = "'{}'".format(attribute)
    priority_level_val = 'NULL'
    if priority_level and priority_level != 'nan':
        priority_level_val = "'{}'".format(priority_level)
    sql = "insert into target (obs_type, radeg, decdeg, req_obs_duration_in_minutes, pointing_attribute, priority, obj_name, obj_source, source_ext_id ) values ('{}',{:.4f},{:.4f},{:.2f}, {}, {}, '{}', '{}', '{}');".format(
        obs_type, float(rad), float(decd), float(req_obs_duration)/60, attribute_val, priority_level_val, obj_name, obj_source, source_ext_id)
    sql_act(db, sql, n=0)
    sql = "select obj_id from target order by obj_id desc limit 1;"
    res = sql_act(db, sql)
    if res:
        return res[0][0]


def write_target_comb_obs(obj_id, tels):
    if tels:
        sql = "update target_combined_observation_rec set status_flag=0 where obj_id={}".format(
            obj_id)
        sql_act(db, sql, n=0)
        telList = tels.strip().split(',')
        for tel in telList:
            if tel.strip() and tel.strip() != 'nan':
                sql = "insert into target_combined_observation_rec (obj_id, combined_observation, status_flag) values({}, '{}', 1)".format(
                    obj_id, tel.strip())
                sql_act(db, sql, n=0)


def write_target_const(obj_id, const_beg, const_end):
    sql = "select * from target_time_allocation where obj_id={}".format(obj_id)
    res = sql_act(db, sql)
    if res:
        sql = "update target_time_allocation set suggested_obs_t_beg='{}', suggested_obs_t_end='{}'".format(
            const_beg, const_end)
    else:
        sql = "insert into target_time_allocation (obj_id, suggested_obs_t_beg, suggested_obs_t_end) values ({}, '{}', '{}')".format(
            obj_id, const_beg, const_end)
    sql_act(db, sql, n=0)


def write_target_obs_params_func(obj_id, inst_id, opc_id, opc_value='NULL'):
    if obj_id and inst_id and opc_id:
        sql = "update target_instrument_rec set status_flag=0 where obj_id={} and inst_id={}".format(
            obj_id, inst_id)
        sql_act(db, sql, n=0)
        sql = "insert into target_instrument_rec (obj_id, inst_id, status_flag) values ({}, {}, 1)".format(
            obj_id, inst_id)
        sql_act(db, sql, n=0)
        sql = "update target_observation_parameters_rec set status_flag=0 where obj_id={} and opc_id={}".format(
            obj_id, opc_id)
        sql_act(db, sql, n=0)
        sql = "insert into target_observation_parameters_rec (obj_id, opc_id, opc_value, status_flag) values ({}, {}, {}, 1)".format(
            obj_id, opc_id, opc_value)
        sql_act(db, sql, n=0)
    elif obj_id and opc_id:
        sql = "update target_observation_parameters_rec set status_flag=0 where obj_id={} and opc_id={}".format(
            obj_id, opc_id)
        sql_act(db, sql, n=0)
        sql = "insert into target_observation_parameters_rec (obj_id, opc_id, opc_value, status_flag) values ({}, {}, {}, 1)".format(
            obj_id, opc_id, opc_value)
        sql_act(db, sql, n=0)


def write_target_obs_params(obj_id, ECL_CONF, GRM_CONF, MXT_CONF, VT_CONF={}, PF_CONF={}):
    '''
        VT_CONF_keys: EXPOSURE_TIME WINDOW_SIZE INTERVAL_BETWEEN_IMG READ_SPEED READ_CHANNEL CLEANING
        PF_CONF_keys: STABILITY MOON_CHECK
    '''

    inst_id = 0
    opc_id = 0
    opc_value = 'NULL'

    # print(ECL_CONF, GRM_CONF, MXT_CONF)
    # print(ECL_CONF.lower(), ECL_CONF.lower() == 'default')

    if ECL_CONF and ECL_CONF.lower() == 'default':
        inst_id = 1
        opc_id = 1
        write_target_obs_params_func(obj_id, inst_id, opc_id)

    if GRM_CONF and GRM_CONF.lower() == 'default':
        inst_id = 2
        opc_id = 2
        write_target_obs_params_func(obj_id, inst_id, opc_id)

    if MXT_CONF and MXT_CONF.lower() != 'not_used':
        inst_id = 3
        if MXT_CONF.lower() == 'default':
            opc_id = 3
            write_target_obs_params_func(obj_id, inst_id, opc_id)
        elif MXT_CONF.lower() == 'filter':
            opc_id = 4
            write_target_obs_params_func(obj_id, inst_id, opc_id)

    if VT_CONF:
        if str(VT_CONF['EXPOSURE_TIME']):
            inst_id = 4
            opc_id = 5
            opc_value = "'{}'".format(int(VT_CONF['EXPOSURE_TIME']))
            write_target_obs_params_func(
                obj_id, inst_id, opc_id, opc_value=opc_value)
        if str(VT_CONF['WINDOW_SIZE']):
            inst_id = 4
            opc_id = 6
            opc_value = "'{}'".format(int(VT_CONF['WINDOW_SIZE']))
            write_target_obs_params_func(
                obj_id, inst_id, opc_id, opc_value=opc_value)
        if str(VT_CONF['INTERVAL_BETWEEN_IMG']):
            inst_id = 4
            opc_id = 7
            opc_value = "'{}'".format(int(VT_CONF['INTERVAL_BETWEEN_IMG']))
            write_target_obs_params_func(
                obj_id, inst_id, opc_id, opc_value=opc_value)
        if str(VT_CONF['READ_SPEED']):
            inst_id = 4
            val = str(int(VT_CONF['READ_SPEED']))
            val_rel = {'1': 8, '2': 9, '3': 10, '4': 11}
            opc_id = val_rel[val]
            write_target_obs_params_func(obj_id, inst_id, opc_id)
        if str(VT_CONF['READ_CHANNEL']):
            inst_id = 4
            val = str(int(VT_CONF['READ_CHANNEL']))
            val_rel = {'0': 12, '1': 13, '2': 14}
            opc_id = val_rel[val]
            write_target_obs_params_func(obj_id, inst_id, opc_id)
        if str(VT_CONF['CLEANING']):
            inst_id = 4
            val = str(int(VT_CONF['CLEANING']))
            val_rel = {'0': 15, '1': 16}
            opc_id = val_rel[val]
            write_target_obs_params_func(obj_id, inst_id, opc_id)

    if PF_CONF:
        if PF_CONF['STABILITY']:
            inst_id = 0
            val = str(PF_CONF['STABILITY'])
            val_rel = {'HIGH': 17, 'NORMAL': 18}  # GENERAL
            opc_id = val_rel[val]
            write_target_obs_params_func(obj_id, inst_id, opc_id)
        if PF_CONF['MOON_CHECK']:
            inst_id = 0
            val = str(PF_CONF['MOON_CHECK'])
            val_rel = {'MXT': 19, 'VT': 20, 'NO-CHECK': 21}
            opc_id = val_rel[val]
            write_target_obs_params_func(obj_id, inst_id, opc_id)


def write_main(data_lines_input):
    '''
        data_line: [obs_type-4, svomdb_id-6, rad-7, decd-8, req_obs_duration-22, ECL_CONF-15, MXT_CONF-14, VT_EXP_TIME-16, VT_WIN_SIZE-17, VT_INTERVAL-18, VT_READ_SPE-19, VT_READ_CHA-20, VT_CLE-21, PF_MOON-36, PF_STA-37, attribute-38, priority-39, combined-40, ###obj_name-41, ###obj_source-42, ###source_ext_id-43, ###user_group-44, ###GRM_CONF-45]
    '''
    global db
    db = con_db()

    # obj_list = []
    catalog_id = write_gp_catalog(
        '0000-00-00T00:00:00Z', '0000-00-00T00:00:00Z')
    for data_line_input in data_lines_input:
        if data_line_input:
            # write_target(obs_type, rad, decd, req_obs_duration, attribute, priority_level, ###obj_name, ###obj_source, ###source_ext_id)
            obj_id = write_target(
                data_line_input[4], data_line_input[7], data_line_input[8], data_line_input[22], data_line_input[38], data_line_input[39], data_line_input[41], data_line_input[42], data_line_input[43])
            # obj_list.append(obj_id)
            if obj_id:
                write_target_comb_obs(obj_id, data_line_input[40]) # combined
                write_gp_catalog_targets(
                    catalog_id, obj_id, data_line_input[6], data_line_input[44]) # svomdb_id user_group
                # write_target_obs_params(obj_id, ECL_CONF, GRM_CONF, MXT_CONF, VT_CONF={}, PF_CONF={}):
                # '''
                #     VT_CONF_keys: EXPOSURE_TIME WINDOW_SIZE INTERVAL_BETWEEN_IMG READ_SPEED READ_CHANNEL CLEANING
                #     PF_CONF_keys: STABILITY MOON_CHECK
                # '''
                VT_CONF = {'EXPOSURE_TIME': data_line_input[16], 'WINDOW_SIZE': data_line_input[17], 'INTERVAL_BETWEEN_IMG': data_line_input[18],
                        'READ_SPEED': data_line_input[19], 'READ_CHANNEL': data_line_input[20], 'CLEANING': data_line_input[21]}
                PF_CONF = {
                    'STABILITY': data_line_input[37], 'MOON_CHECK': data_line_input[36]}
                write_target_obs_params(
                    obj_id, data_line_input[15], data_line_input[45], data_line_input[14], VT_CONF, PF_CONF)
    # for obj in obj_list:
        # write_gp_catalog_targets(catalog_id, obj, '')

    close_db(db)

    return catalog_id



def write_main_more(data_lines_input):
    global db
    db = con_db()

    # obj_list = []
    catalog_id = write_gp_catalog(
        '0000-00-00T00:00:00Z', '0000-00-00T00:00:00Z')
    for line in data_lines_input:
        svomdb_id = line[0]
        obj_name = line[1]
        obj_source = line[2]
        source_ext_id = line[3]
        obs_type = line[4]
        rad = line[5]
        decd = line[6]
        req_obs_duration = line[7]
        attribute = line[8]
        priority_level = line[9]
        combined = line[10]
        user_group = line[11]
        
        ecl_conf = line[12]
        grm_conf = line[13]
        mxt_conf = line[14]
        vt_exp_time = line[15]
        vt_win_size = line[16]
        vt_interval = line[17]
        vt_read_spe = line[18]
        vt_read_cha = line[19]
        vt_read_cl = line[20]
        pf_sta = line[21]
        pf_moon = line[22]

        obj_id = write_target(obs_type, rad, decd, req_obs_duration, attribute, priority_level, obj_name, obj_source, source_ext_id)
        # obj_list.append(obj_id)
        if obj_id:
            write_target_comb_obs(obj_id, combined)
            write_gp_catalog_targets(
                catalog_id, obj_id, svomdb_id, user_group)
            # write_target_obs_params(obj_id, ECL_CONF, GRM_CONF, MXT_CONF, VT_CONF={}, PF_CONF={}):
            # '''
            #     VT_CONF_keys: EXPOSURE_TIME WINDOW_SIZE INTERVAL_BETWEEN_IMG READ_SPEED READ_CHANNEL CLEANING
            #     PF_CONF_keys: STABILITY MOON_CHECK
            # '''
            VT_CONF = {'EXPOSURE_TIME': vt_exp_time, 'WINDOW_SIZE': vt_win_size, 'INTERVAL_BETWEEN_IMG': vt_interval,
                       'READ_SPEED': vt_read_spe, 'READ_CHANNEL': vt_read_cha, 'CLEANING': vt_read_cl}
            PF_CONF = {
                'STABILITY': pf_sta, 'MOON_CHECK': pf_moon}
            write_target_obs_params(
                obj_id, ecl_conf, grm_conf, mxt_conf, VT_CONF, PF_CONF)
    # for obj in obj_list:
        # write_gp_catalog_targets(catalog_id, obj, '')

    close_db(db)

    return catalog_id


def updt_gp_catalog(catalog_id, time_beg, time_end):
    db = con_db()
    sql = "update gp_catalog set window_begin='{}', window_end='{}' where gp_catalog_id={}".format(
        time_beg, time_end, catalog_id)
    sql_act(db, sql, n=0)
    close_db(db)


if __name__ == '__main__':
    data_lines_input = [['02/01/2022 03:25:17', 17.0, 29.0, 12.0, 'GP-PPT', 16.0, 'GP_67', 80.5376, -26.524, 0.308841717, 0.691494884, 0.652268514, -0.031581714, 'nan', 'FILTER', 'DEFAULT', 15.0,
                         300.0, 0.0, 1.0, 0.0, 0.0, 30742.0, 80.5376, 26.524, 154.4484, 126.0579, 124.0582, 90.0, 90.0, 143.9421, 126.1987, 125.8509, 90.3292, 89.6708, 143.7993, 'VT', 'HIGH', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan', 'nan']]
    catalog_id = write_main(data_lines_input)
    print('Catalog ID:' + str(catalog_id))
    catalog_window_beg = '0000-00-00T00:00:00Z'
    catalog_window_end = '0000-00-00T00:00:00Z'
    updt_gp_catalog(catalog_id, catalog_window_beg, catalog_window_end)
