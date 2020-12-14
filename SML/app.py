import sys
from flask import Flask,jsonify
from MySQLShipDate import except_dict


# 导入公司的蓝图
from SML_Blueprint.SML_Blueprint import SML_Blueprint

app = Flask(__name__)

# 注册蓝图
app.register_blueprint(SML_Blueprint)


if __name__ == '__main__':
    # 检查数据库是否能正常使用
    if set(except_dict.values()) != {'OK'}:
        for key, value in except_dict.items():
            except_dict[key] = str(value)
        print(jsonify(except_dict))
    else:
        if sys.platform in ['win32','win64']:
            app.run(host='0.0.0.0',debug=True)
        else:
            app.run(debug=True)
        pass

