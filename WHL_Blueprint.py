from flask import Blueprint,request,jsonify
import os,sys,re
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime
from MySQLShipDate import carrier,port,dictionary
now = os.path.dirname(__file__)
last_path = os.path.abspath(os.path.join(now,os.path.pardir))
sys.path.append(last_path)

WHL_Blueprint = Blueprint('WHL_blueprint',__name__)


@WHL_Blueprint.route('/WHL_POST',methods=['POST'])
def get_json():
    file = request.files.get('file')
    if file:
        WHL_div_Path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'shipfroms'))
        file_name = secure_filename(file.filename)
        file_path = os.path.abspath(os.path.join(WHL_div_Path,file_name))
        file.save(file_path)

        data = pd.read_excel(file_path,None)
        sheet_name = list(data.keys())
        df_list = []
        for name in sheet_name:
            df = data[name]
            sheet_start_list = []
            sheet_end_list = []
            row = 0
            while row < df.shape[0]:
                while row < df.shape[0]:
                    if ('VESSEL'==df.iloc[row,0] or 'VOY'==df.iloc[row,2] or'船名'==df.iloc[row,0] or '船名\nVESSEL NAME'==df.iloc[row,0] or 'VESSEL NAME'==df.iloc[row,0] ):
                        r = row - 2
                        if r < 0:
                            sheet_start_list.append(row - 1)
                        else:
                            sheet_start_list.append(row - 2)
                        break
                    row +=1

                row = row+2
                while row < df.shape[0]:
                    if sum(df.iloc[row,:].notna())>=1:
                        c = row +1
                        if c>df.shape[0]-1:
                            sheet_end_list.append(row)
                            break
                        else:
                            row = row + 1
                    else:
                        sheet_end_list.append(row)
                        break
                row = row + 1

            for i in range(len(sheet_end_list)):
                dataframe = df.iloc[sheet_start_list[i]:sheet_end_list[i], :]
                value = dataframe.iloc[0,0]
                if pd.isnull(value) or len(str(value)) >= 8 :
                    code = str(dataframe.iloc[1,0]).split(' ')[0]
                else:
                    code = str(dataframe.iloc[0, 0]).split(' ')[0]
                code = re.findall('[A-Za-z0-9()-]',code)
                code = ''.join(code)

                # 搜索需要的列：'VESSEL','VOY','ETD','VESSEL NAME'
                dataframe = pd.DataFrame(dataframe)
                dataframe.dropna(axis=0, how='any', thresh=4, inplace=True)

                col_index = []
                for c in range(dataframe.shape[1]):
                    col_data = list(dataframe.iloc[:, c])
                    if ('VESSEL' in col_data) or ('船名' in col_data) or ('VESSEL NAME' in col_data) or ('船名\nVESSEL NAME' in col_data):
                        col_index.append(c)
                    elif ('VOY' in col_data) or ('航次' in col_data) or ('AMS航次' in col_data) or ('VOYAGE' in col_data):
                        col_index.append(c)
                    elif ('ETD' in col_data) or ('開航' in col_data) or ('開航日\nETD' in col_data):
                        col_index.append(c)

                for c in range(dataframe.shape[1]):
                    col_data = list(dataframe.iloc[:, c])
                    if ('船 名' in col_data) or ('VESSEL NAME' in col_data):
                        col_index.append(c+1)
                dataframe = dataframe.iloc[:, col_index]
                dataframe.index = range(dataframe.shape[0])
                # 删除有空缺值的行
                # for r in range(dataframe.shape[0]):
                #     if sum(dataframe.loc[r, :].isna()):
                #         dataframe.drop(index=r, inplace=True)
                # 规范标题行和索引
                if dataframe.shape[1]>3:
                    dataframe.columns = ['VESSEL', 'VOY', 'ETD','VESEEL NAME']
                else:
                    dataframe.columns = ['VESSEL', 'VOY', 'ETD']

                for r in range(dataframe.shape[0]):
                    if sum(dataframe.loc[r, :].isna()):
                        dataframe.drop(index=r, inplace=True)

                dataframe.index = range(dataframe.shape[0])
                for r in range(dataframe.shape[0]):
                    if re.findall('^[A-Z]*$',dataframe.loc[r,'VOY']):
                        dataframe.drop(index=r, inplace=True)
                dataframe.index = range(dataframe.shape[0])

                for r in range(dataframe.shape[0]):
                    if re.findall('[\u4e00-\u9fa5]', dataframe.loc[r, 'VESSEL']):
                        dataframe.drop(index=r, inplace=True)
                dataframe.index = range(dataframe.shape[0])
                # 保存处理好的数据到列表
                df_list.append((code, dataframe))

        # 转换数据
        count = 0
        items_list = []
        no_start_port_id_list = []
        for code, df in df_list:
            for record_index in range(df.shape[0]):
                item_dict = {}
                record = df.iloc[record_index, :]
                if code != code:
                    item_dict['code'] = ' '
                else:
                    item_dict['code'] = code
                item_dict['data_from'] = '2'
                item_dict['etd'] = str(record['ETD']).split()[0]
                item_dict['schedule'] = datetime.strptime(item_dict['etd'], '%Y-%m-%d').weekday() + 1
                if 'WHL' in carrier.keys():
                    item_dict['scid'] = carrier['WHL']
                elif 'WHL' in dictionary.keys():
                    item_dict['scid'] = dictionary['WHL']
                else:
                    item_dict['scid'] = 'WHL'
                    return '找不到船公司ID！！'
                item_dict['ship_name'] = record['VESSEL']
                if('VESEEL NAME' in record.keys()):
                    item_dict['ship_namecn'] = record['VESEEL NAME']
                # 默认上海
                WHL_port = 'shanghai'.upper()
                if WHL_port in port.keys():
                    item_dict['start_port_id'] = port[WHL_port]
                elif WHL_port in dictionary.keys():
                    item_dict['start_port_id'] = dictionary[WHL_port]
                else:
                    item_dict['start_port_id'] = WHL_port
                    no_start_port_id_list.append(WHL_port)
                    continue
                item_dict['voyage'] = record['VOY']
                items_list.append(item_dict)
                count += 1
        return_json = {"count": count, "items": items_list,
                       'no_start_port_id_list': list(set(no_start_port_id_list))}
        return jsonify(return_json)

        # return file_path
    else:
        return '文件没有传进'
