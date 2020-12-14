from flask import Blueprint,request,jsonify
import os,sys,xlrd
from werkzeug.utils import   secure_filename
import pandas as pd
from datetime import datetime
import re
from MySQLShipDate import carrier,port,dictionary,except_dict,port_chinese

now = os.path.dirname(__file__) #当前路径
last_path = os.path.abspath(os.path.join(now,os.path.pardir)) # 上一级目录
sys.path.append(last_path)  # 添加可导包目录


SML_Blueprint = Blueprint('SML_blueprint',__name__)


@SML_Blueprint.route('/SML_POST',methods=['POST'])
def get_json():
    #获取文件
    file=request.files.get('file')
    if file:
        #保存文件
        # os.rename
        file_save_dir_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'shipfroms'))
        file_name=secure_filename(file.filename)
        file_path=os.path.abspath(os.path.join(file_save_dir_path,file_name))
        file.save(file_path)
        # return file_path

        try:
            data = pd.read_excel(file_path, None)
        except:
            # HTML
            data = pd.read_html(file_path)[0]
            return SML_HTML(data)
        else:
            # EXCEL
            data = pd.read_excel(file_path,None)
            return SML_EXCEL(data,file_path)
    else:
        return '没有传入文件'

def SML_HTML(data):
        data1 = data.loc[:, ['Cargo Closing Time','Loading Port','Departure Date','Arrival','Lane','Vessel']]

        if len(data1.iloc[0, 4]) > 5 and len(data1.iloc[1, 4]) > 5:
            dataframe1 = pd.DataFrame(columns=list("ABCDEF"))
            dataframe2 = pd.DataFrame(columns=list("ABCD"))
            for i in range(data1.shape[0]):
                da1 = []
                da2 = []
                CCT1_time = str(data1.iloc[i,0]).split(' ')[0]
                da1.append(CCT1_time)
                post1 = str(data1.iloc[0,1]).replace('PORT','').split(' ')[0]
                da1.append(post1)
                ETD1_time = str(data1.iloc[i,2]).split(' ')[0]
                da1.append(ETD1_time)
                ETA1_time = str(data1.iloc[i,3]).split(' ')[0]
                da1.append(ETA1_time)
                code1 = str(data1.iloc[i,4]).split('FDR')[0]
                da1.append(code1)
                vessel1 = str(data1.iloc[i,5]).split('Feeder')[0]
                da1.append(vessel1)

                da2.append(CCT1_time)
                da2.append(post1)
                ETD2_time = str(data1.iloc[i, 2]).split(')')[1].split(' ')[0]
                da2.append(ETD2_time)
                ETA2_time = str(data1.iloc[i, 3]).split(')')[1].split(' ')[0]
                da2.append(ETA2_time)

                dataframe1.loc[len(dataframe1)] = da1
                dataframe2.loc[len(dataframe1)] = da2
            newdata = pd.concat([dataframe1,dataframe2])
            newdata.columns = ['Cargo Closing Time', 'POST', 'ETD', 'ETA','CODE','VESSEL']

            count = 0
            items_list = []
            no_id_start_port_list = []
            for record_index in range(newdata.shape[0]):
                item_dict = {}
                record = newdata.iloc[record_index, :]
                if record['CODE'] != record['CODE']:
                    item_dict['code'] = ''
                else:
                    item_dict['code'] = record['CODE']
                item_dict['data_from'] = '2'
                item_dict['Cargo Closing Time'] = record['Cargo Closing Time']
                item_dict['etd'] = record['ETD']
                item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
                item_dict['eta'] = record['ETA']
                #查询数据库中船公司的id
                carrier_id = 'SML'
                if carrier_id in carrier.keys():
                    item_dict['scid'] = carrier[carrier_id]
                elif carrier_id in dictionary.keys():
                    item_dict['scid'] = dictionary[carrier_id]
                else:
                    return '找不到船公司ID'

                a = str(record['VESSEL']).split()
                voyage = a[-1]
                ship_name = str(record['VESSEL']).replace(voyage, '')
                ship_name = ship_name.rstrip()
                if ship_name != ship_name:
                    item_dict['ship_name'] = ''
                else:
                    item_dict['ship_name'] = ship_name
                # 查询数据库中起运港的id
                POL = record['POST'].upper()
                if POL in port.keys():
                    item_dict['start_port_id'] = port[POL]
                elif POL in dictionary.keys():
                    item_dict['start_port_id'] = dictionary[POL]
                else:
                    no_id_start_port_list.append(POL)
                    continue
                if pd.isna(record['VESSEL']):
                    item_dict['voyage'] = ''
                else:
                    item_dict['voyage'] = voyage
                # item_dict['voyage'] = voyage
                items_list.append(item_dict)
                count += 1
            return_json = {'count': count, 'items': items_list, 'no_id_start_port_list': list(set(no_id_start_port_list))}
            return jsonify(return_json)
            # return file_path
        else:
            count = 0
            items_list = []
            no_id_start_port_list = []
            for record_index in range(data1.shape[0]):
                item_dict = {}
                record = data1.iloc[record_index, :]
                item_dict['code'] = record[4]
                item_dict['data_from'] = '2'
                item_dict['Cargo Closing Time'] = record[0].split()[0]
                item_dict['etd'] = record[2].split()[0]
                item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
                item_dict['eta'] = record[3].split()[0]
                #查询数据库中船公司的id
                carrier_id = 'SML'
                if carrier_id in carrier.keys():
                    item_dict['scid'] = carrier[carrier_id]
                elif carrier_id in dictionary.keys():
                    item_dict['scid'] = dictionary[carrier_id]
                else:
                    return '找不到船公司ID'

                a = record[-1].split()
                voyage = a[-1]
                ship_name = record[-1].replace(voyage, '')
                ship_name = ship_name.rstrip()
                item_dict['ship_name'] = ship_name
                # 查询数据库中起运港的id
                POL = record[1].upper()
                if POL in port.keys():
                    item_dict['start_port_id'] = port[POL]
                elif POL in dictionary.keys():
                    item_dict['start_port_id'] = dictionary[POL]
                else:
                    no_id_start_port_list.append(POL)
                    continue
                item_dict['voyage'] = voyage
                items_list.append(item_dict)
                count += 1
            return_json = {'count': count, 'items': items_list, 'no_id_start_port_list': list(set(no_id_start_port_list))}
            return jsonify(return_json)
            # return file_path
def SML_EXCEL(data,file_path):
    if len(data.keys()) == 1:
        for key in data.keys():
            data1 = data[key]
            data1 = data1.iloc[:, [0, 1, 2, 4, 5, 6]]
            data1 = pd.DataFrame(data1)
            data1.dropna(axis=0, how='any', thresh=3, inplace=True)
            data1.columns = ['Cargo Closing Time', 'Loading Port', 'Departure Date', 'Arrival', 'Lane', 'Vessel']
            data1.index = range(data1.shape[0])

            if len(data1.loc[0, 'Lane']) > 5 and len(data1.loc[1, 'Lane']) > 5:
                dataframe1 = pd.DataFrame(columns=list("ABCDEF"))
                dataframe2 = pd.DataFrame(columns=list("ABCD"))
                for i in range(data1.shape[0]):
                    da1 = []
                    da2 = []
                    CCT1_time = data1.loc[i, 'Cargo Closing Time'].split(' ')[0]
                    da1.append(CCT1_time)
                    post1 = data1.loc[i, 'Loading Port'].replace('PORT', '').split(' ')[0]
                    da1.append(post1)
                    ETD1_time = data1.loc[i, 'Departure Date'].split(' ')[0]
                    da1.append(ETD1_time)
                    ETA1_time = data1.loc[i, 'Arrival'].split(' ')[0]
                    da1.append(ETA1_time)
                    code1 = data1.loc[i, 'Lane'].split('FDR')[0]
                    da1.append(code1)
                    vessel1 = data1.loc[i, 'Vessel'].split('Feeder')[0]
                    da1.append(vessel1)

                    da2.append(CCT1_time)
                    da2.append(post1)
                    ETD2_time = data1.loc[i, 'Departure Date'].split(')')[1].split(' ')[0]
                    da2.append(ETD2_time)
                    ETA2_time = data1.loc[i, 'Arrival'].split(')')[1].split(' ')[0]
                    da2.append(ETA2_time)

                    dataframe1.loc[len(dataframe1)] = da1
                    dataframe2.loc[len(dataframe1)] = da2
                newdata = pd.concat([dataframe1, dataframe2])
                newdata.columns = ['Cargo Closing Time', 'POST', 'ETD', 'ETA', 'CODE', 'VESSEL']

                count = 0
                items_list = []
                no_id_start_port_list = []
                for record_index in range(newdata.shape[0]):
                    item_dict = {}
                    record = newdata.iloc[record_index, :]
                    if record['CODE'] != record['CODE']:
                        item_dict['code'] = ''
                    else:
                        item_dict['code'] = record['CODE']
                    item_dict['data_from'] = '2'
                    item_dict['Cargo Closing Time'] = record['Cargo Closing Time']
                    item_dict['etd'] = record['ETD']
                    item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
                    item_dict['eta'] = record['ETA']
                    # 查询数据库中船公司的id
                    carrier_id = 'SML'
                    if carrier_id in carrier.keys():
                        item_dict['scid'] = carrier[carrier_id]
                    elif carrier_id in dictionary.keys():
                        item_dict['scid'] = dictionary[carrier_id]
                    else:
                        return '找不到船公司ID'

                    a = str(record['VESSEL']).split()
                    voyage = a[-1]
                    ship_name = str(record['VESSEL']).replace(voyage, '')
                    ship_name = ship_name.rstrip()
                    if ship_name != ship_name:
                        item_dict['ship_name'] = ''
                    else:
                        item_dict['ship_name'] = ship_name
                    # 查询数据库中起运港的id
                    POL = record['POST'].upper()
                    if POL in port.keys():
                        item_dict['start_port_id'] = port[POL]
                    elif POL in dictionary.keys():
                        item_dict['start_port_id'] = dictionary[POL]
                    else:
                        no_id_start_port_list.append(POL)
                        continue
                    if pd.isna(record['VESSEL']):
                        item_dict['voyage'] = ''
                    else:
                        item_dict['voyage'] = voyage
                    items_list.append(item_dict)
                    count += 1
                return_json = {'count': count, 'items': items_list,
                               'no_id_start_port_list': list(set(no_id_start_port_list))}
                return jsonify(return_json)
            else:
                for i in range(data1.shape[0]):
                    if pd.isna(data1.loc[i, 'Cargo Closing Time']):
                        data1.loc[i, 'Cargo Closing Time'] = data1.loc[i - 1, 'Cargo Closing Time']
                    else:
                        data1.loc[i, 'Cargo Closing Time'] = data1.loc[i, 'Cargo Closing Time']

                count = 0
                items_list = []
                no_id_start_port_list = []
                for record_index in range(data1.shape[0]):
                    record = data1.iloc[record_index, :]
                    item_dict = {}
                    if record['Lane'] == 'FDR':
                        item_dict['code'] = ''
                    else:
                        item_dict['code'] = record['Lane']
                    item_dict['data_from'] = '2'
                    item_dict['Cargo Closing Time'] = str(record['Cargo Closing Time']).split()[0]
                    item_dict['etd'] = str(record['Departure Date']).split()[0]
                    item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
                    item_dict['eta'] = str(record['Arrival']).split()[0]
                    # 查询数据库中船公司的id
                    carrier_id = 'SML'
                    if carrier_id in carrier.keys():
                        item_dict['scid'] = carrier[carrier_id]
                    elif carrier_id in dictionary.keys():
                        item_dict['scid'] = dictionary[carrier_id]
                    else:
                        return '找不到船公司ID'
                    a = str(record['Vessel']).split()
                    voyage = a[-1]
                    ship_name = str(record['Vessel']).replace(voyage, '')
                    ship_name = ship_name.rstrip()
                    if ship_name == 'Feeder(Not':
                        item_dict['ship_name'] = ''
                    else:
                        item_dict['ship_name'] = ship_name
                    # 查询数据库中起运港的id
                    POL = record['Loading Port'].upper()
                    if POL in port.keys():
                        item_dict['start_port_id'] = port[POL]
                    elif POL in dictionary.keys():
                        item_dict['start_port_id'] = dictionary[POL]
                    else:
                        no_id_start_port_list.append(POL)
                        continue
                    if voyage == 'Assigned)':
                        item_dict['voyage'] = ''
                    else:
                        item_dict['voyage'] = voyage
                    items_list.append(item_dict)
                    count += 1
                return_json = {'count': count, 'items': items_list,
                               'no_id_start_port_list': list(set(no_id_start_port_list))}
                return jsonify(return_json)
    else:
        data2=pd.DataFrame()
        for i in data.keys():
            data = pd.read_excel(file_path,sheet_name=i)
            data1 = data.iloc[:, [0, 1, 2, 4, 5, 6]]
            data1 = pd.DataFrame(data1)
            data1.dropna(axis=0, how='any', thresh=3, inplace=True)
            data1.columns = ['Cargo Closing Time', 'Loading Port', 'Departure Date', 'Arrival', 'Lane', 'Vessel']
            data1.index = range(data1.shape[0])
            for i in range(data1.shape[0]):
                if pd.isna(data1.loc[i, 'Cargo Closing Time']):
                    data1.loc[i, 'Cargo Closing Time'] = data1.loc[i - 1, 'Cargo Closing Time']
                else:
                    data1.loc[i, 'Cargo Closing Time'] = data1.loc[i, 'Cargo Closing Time']
            data2 = data2.append(data1)
        count = 0
        items_list = []
        no_id_start_port_list = []
        for record_index in range(data2.shape[0]):
            record = data2.iloc[record_index, :]
            item_dict = {}
            if record['Lane'] == 'FDR':
                item_dict['code'] = ''
            else:
                item_dict['code'] = record['Lane']
            item_dict['data_from'] = '2'
            item_dict['Cargo Closing Time'] = str(record['Cargo Closing Time']).split()[0]
            item_dict['etd'] = str(record['Departure Date']).split()[0]
            item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
            item_dict['eta'] = str(record['Arrival']).split()[0]
            # 查询数据库中船公司的id
            carrier_id = 'SML'
            if carrier_id in carrier.keys():
                item_dict['scid'] = carrier[carrier_id]
            elif carrier_id in dictionary.keys():
                item_dict['scid'] = dictionary[carrier_id]
            else:
                return '找不到船公司ID'
            a = str(record['Vessel']).split()
            voyage = a[-1]
            ship_name = str(record['Vessel']).replace(voyage, '')
            ship_name = ship_name.rstrip()
            if ship_name == 'Feeder(Not':
                item_dict['ship_name'] = ''
            else:
                item_dict['ship_name'] = ship_name
            # 查询数据库中起运港的id
            POL = record['Loading Port'].upper()
            if POL in port.keys():
                item_dict['start_port_id'] = port[POL]
            elif POL in dictionary.keys():
                item_dict['start_port_id'] = dictionary[POL]
            else:
                no_id_start_port_list.append(POL)
                continue
            if voyage == 'Assigned)':
                item_dict['voyage'] = ''
            else:
                item_dict['voyage'] = voyage
            items_list.append(item_dict)
            count += 1
        return_json = {'count': count, 'items': items_list,
                       'no_id_start_port_list': list(set(no_id_start_port_list))}
        return jsonify(return_json)
