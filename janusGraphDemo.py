# -*- coding: utf-8 -*-
# @Time   : 2022/02/09 16:52
# @Author : yuanhp
# @Project : janusgraph
# @FileName: janusGraphDemo.py
# @Desc : ==============================================
# 用gremlin_python 导入小批量的数据到janusgraph
# ======================================================
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
import numpy as np
def addVertex(file):
    # g.addV('god').property('name', 'yuxj').property('age', 111).next()
    # a = g.V().has('name', 'yuxj').valueMap().toList()
    # print(a)
    netConn_key=['vertex_id','vertex_type','uid','protocol','service','duration','origBytes','respBytes','connState','missedBytes','history','origPkts','origIpBytes','respPkts','respIpBytes']
    host_key=['vertex_id','vertex_type','ip']
    port_key=['vertex_id','vertex_type','port']
    #g.addV('god').property('name', 'yuxj').property('age', 111)
    with open(file, "r") as f:
        lines = f.readlines()
        for line in lines:
            line_splits=line.strip().split(',')
            label=line_splits[1]
            vert = g.addV(label)  # 添加标签
            vert.property("vertex_id",line_splits[0])
            if label =='NetConn':
                data=dict(zip(netConn_key, line_splits))
                for key in netConn_key[2:len(netConn_key)]:
                    vert.property(key,data[key])
            if label =='hostname':
                data=dict(zip(host_key, line_splits))
                for key in host_key[2:len(netConn_key)]:
                    vert.property(key,data[key])
            if label =='port':
                data = dict(zip(port_key, line_splits))
                for key in port_key[2:len(port_key)]:
                    vert.property(key, data[key])
            vert.next()

def addEdge(file):
    edge_key=['src_id','edge_label','dst_id']
    with open(file, "r") as f:
        lines=f.readlines()
        for line in lines:
            line_split=line.strip().split(',')
            data=dict(zip(edge_key,line_split))
            edge_label=data['edge_label']
            v_start = g.V().has("vertex_id",data['src_id'])
            v_end =g.V().has("vertex_id",data['dst_id'])
            # edge=g.V(line_split[0]).addE(edge_label).to(line_split[1])  # 添加边标签
            if v_start.hasNext() and v_end.hasNext():
                edge = g.V(v_start.next()).addE(edge_label).to(v_end.next())  # 添加边标签
            else:
                continue
            # 遍历添加边属性如果有,key为属性字段,tmp为值
            property_keys = edge_key
            for key in property_keys:
                tmp = data[key]
                if tmp is np.nan:
                    tmp = ''
                edge.property(key, str(tmp))

            edge.next()
        pass

if __name__ == '__main__':
    graph = Graph()
    clien=DriverRemoteConnection('ws://10.10.15.15:8183/gremlin', 'g')
    g = graph.traversal().withRemote(clien)  # 这里的ip端口就是上面查的
    # addVertex('testv.csv')
    # addEdge('teste.csv')
    print(g.V().valueMap(True).toList())
    e=g.E().valueMap(True).toList()
    print(e)
    v=g.V().has("vertex_id","1").valueMap(True).toList()
    e2=g.V().has("vertex_id",'1').bothE().valueMap(True).toList()
    print(e2)
    clien.close()

