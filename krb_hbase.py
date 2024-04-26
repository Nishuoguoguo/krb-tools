import json

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
from hbase.ttypes import Mutation
from loguru import logger as logging

class hbase_data_util(object):
    def __init__(self,HBASE_ADDRESS,HBASE_PORT):
        self.HBASE_ADDRESS = HBASE_ADDRESS
        self.HBASE_PORT = HBASE_PORT
        sock = TSocket.TSocket(self.HBASE_ADDRESS, self.HBASE_PORT)
        # Set up a SASL transport.
        self.transport = TTransport.TSaslClientTransport(sock, HBASE_ADDRESS, 'hbase')
        self.transport.open()
        # Use the Binary protocol (must match your Thrift server's expected protocol)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.connection = Hbase.Client(protocol)
        self.hbase_family = 'wa'

    def __del__(self):
        self.transport.close()

    def con_ping(self):
        sock = TSocket.TSocket(self.HBASE_ADDRESS, self.HBASE_PORT)
        # Set up a SASL transport.
        self.transport = TTransport.TSaslClientTransport(sock, self.HBASE_ADDRESS, 'hbase')
        self.transport.open()
        # Use the Binary protocol (must match your Thrift server's expected protocol)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.connection = Hbase.Client(protocol)

    def get_hbase_info(self, table_name, rowkeys=None):
        try:
            result = self.connection.getRow(table_name, rowkeys)
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            result = self.connection.getRow(table_name, rowkeys)
        return result

    def get_hbase_str_info(self, table_name, rowkeys=None):
        try:
            result = self.connection.getRow(table_name, rowkeys)
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接: %s" % str(e))
            self.con_ping()
            result = self.connection.getRow(table_name, rowkeys)
        result_list = []
        for data in result:
            result_dict = {}
            for key, value in data[1].items():
                key = key.decode()
                key = key.split("wa:")[1]
                result_dict[key] = value.decode()
            result_list.append(result_dict)
        return result_list

    def get_hbase_by_rowkey(self, table_name, rowkey=None, columns=None):
        try:
            result = self.connection.get(table_name, rowkey, columns)
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            result = self.connection.get(table_name, rowkey, columns)
        if None == result or len(result.keys()) == 0:
            return None
        result_dict = {}
        for key, value in result.items():
            key = key.decode()
            key = key.split("wa:")[1]
            result_dict[key] = value.decode()
        return result_dict

    def get_hbase_table_list(self):
        try:
            return self.connection.getTableNames()
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            return self.connection.getTableNames()

    def pub_hbase_publish_time(self, table_name, rowkey, s_publish_time):
        try:
            return self.connection.mutateRow(table_name, rowkey,
                                             [Mutation(column="wa:s_publish_time", value=s_publish_time)])
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            return self.connection.mutateRow(table_name, rowkey,
                                             [Mutation(column="wa:s_publish_time", value=s_publish_time)])

    def put(self, table_name, rowkey, json_data):
        try:
            mutations = []
            for key in json_data:
                mutations.append(Mutation(column=self.hbase_family + ':' + key, value=json.dumps(json_data[key])))
            result = self.connection.mutateRow(table_name, rowkey, mutations)
            return True
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            mutations = []
            for key in json_data:
                mutations.append(Mutation(column=self.hbase_family + ':' + key, value=json.dumps(json_data[key])))
            try:
                result = self.connection.mutateRow(table_name, rowkey, mutations)
                return True
            except Exception as e:
                logging.error(e)
                return False


    def get(self, table_name, rowkey):
        try:
            row = self.connection.getRow(table_name, rowkey)
            if len(row) == 0:
                return {}
            else:
                _info = self.connection.getRow(table_name, rowkey)[0].columns
                result = {}
                for key in _info:
                    result[key.replace(self.hbase_family + ':', '')] = json.loads(_info[key].value)
                return result
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            row = self.connection.getRow(table_name, rowkey)
            if len(row) == 0:
                return {}
            else:
                _info = self.connection.getRow(table_name, rowkey)[0].columns
                result = {}
                for key in _info:
                    result[key.replace(self.hbase_family + ':', '')] = json.loads(_info[key].value)
                return result

    def get_rows(self, table_name, rowkey):
        try:
            row = self.connection.getRow(table_name, rowkey)
            if len(row) == 0:
                return {}
            else:
                _info = self.connection.getRow(table_name, rowkey)[0].columns
                result = {}
                for key in _info:
                    result[key.replace(self.hbase_family + ':', '')] = json.loads(_info[key].value)
                return result
        except Exception as e:
            logging.error("HBASE获取数据失败，重新初始化链接")
            self.con_ping()
            row = self.connection.getRow(table_name, rowkey)
            if len(row) == 0:
                return {}
            else:
                _info = self.connection.getRow(table_name, rowkey)[0].columns
                result = {}
                for key in _info:
                    result[key.replace(self.hbase_family + ':', '')] = json.loads(_info[key].value)
                return result
