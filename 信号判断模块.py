# 当高级别k线出现交易信号时，在出现信号到下一个周期之前的低级别k线出现同类交易信号后，继续迭代


import time
import matplotlib.pyplot as plt
import talib
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', None)

df = pd.read_csv(r'C:\Users\l\Desktop\data\99999.txt', sep='\t')  # 读取历史数据文本格式

def k_resem(df, num=1):  # 将低级别k线组装为高级别k线（s时刻的n级kbar说明的是s时刻以前的n级周期内的信息。三点收盘后无信息。）
    # 处理流程，首先在df1创建n列，赋值为索引值
    # 创建空df1 df1x
    # 在df1创建tst列，使用cut函数给相应固定长度为num的列赋值
    # 将df1在tst列上进行分类，提取每一类相应的值给与df1x
    # 提取tst列分类上n列的结果给与df1x作为索引
    # 取出在df1上创建的tst和n列，保持df1不变
    # 返回一个df1x和num的元组
    df1 = df.copy()
    df1['n'] = df1.index
    df1x = pd.DataFrame()
    # 在index上使用cut方法，按固定的kbar数量（参数num）切分，切分后的每一组数据集在tst列获得相同列名（每组不同）
    df1['tst'] = pd.cut(df1.index, right=False, bins=range(len(df1))[::num])
    # 按照tst列不同的值进行分类，提取每一类o列第一个数据作为高级别的kbar开盘价
    df1x['o'] = df1.groupby('tst')['o'].first()
    # 按照tst列不同的值进行分类，提取每一类c列最后一个数据作为高级别的kbar收盘价
    df1x['c'] = df1.groupby('tst')['c'].last()
    # 按照tst列不同的值进行分类，提取每一类h列最大值的数据作为高级别的kbar最高价
    df1x['h'] = df1.groupby('tst')['h'].max()
    # 按照tst列不同的值进行分类，提取每一类l列最小值的数据作为高级别的kbar最低价
    df1x['l'] = df1.groupby('tst')['l'].min()
    # 按照tst列不同的值进行分类，提取每一类trade_date列最后一位数据作为高级别的kbar收盘时间
    df1x['trade_date'] = df1.groupby('tst')['trade_date'].last()
    df1x.index = df1.groupby('tst')['n'].last()  # 去除tst列，重设自然数索引
    return (df1x, num)

def macd(df, f, s, a):  # 函数返回k线对应的macd数据，快线为macd，钝线为macdsignal，差值为macdhist，金叉死叉信号为x，
    df1 = df.copy()
    df1t = talib.MACD(df1.c, fastperiod=f, slowperiod=s,
                     signalperiod=a)  # 返回对象为三个series组成的元组
    df1['macd'] = df1t[0]  # 元组元素1
    df1['macdsignal'] = df1t[1]  # 2
    df1['macdhist'] = df1t[2]  # 3
    # macdhist某行与前一行的乘积大于0时同号，x列值为0；乘积小于0时说明不同号，x列值为1，动作确认完成。
    df1['x'] = (df1['macdhist'].shift(1)*df1['macdhist']
               ).apply(lambda x: 1 if x < 0 else 0)
    df1.fillna(0,inplace  =True)  # 填补shift产生的nan
    df1['xx'] = (df1['macdhist'] * df1['x']).apply(lambda x: -1 if x < 0 else (0 if x==0 else 1))
    df1['x'] = df1['xx']
    df1.drop('xx',inplace = True, axis=1)
    df1['x'] = df1['x'].shift(1)
    df1.fillna(0,inplace  =True)
    return df1

def macd(df, f, s, a):  # 函数返回k线对应的macd数据，快线为macd，钝线为macdsignal，差值为macdhist，金叉死叉信号为x，

    dft = talib.MACD(df.c, fastperiod=f, slowperiod=s,
                     signalperiod=a)  # 返回对象为三个series组成的元组
    df['macd'] = dft[0]  # 元组元素1
    df['macdsignal'] = dft[1]  # 2
    df['macdhist'] = dft[2]  # 3
    # macdhist某行与前一行的乘积大于0时同号，x列值为0；乘积小于0时说明不同号，x列值为1，动作确认完成。
    df['x'] = (df['macdhist'].shift(1)*df['macdhist']
               ).apply(lambda x: 1 if x < 0 else 0)
    df.fillna(0, inplace=True)  # 填补shift产生的nan
    df['xx'] = (df['macdhist'] * df['x']).apply(lambda x: -
                                                1 if x < 0 else (0 if x == 0 else 1))
    df['x'] = df['xx']
    df.drop('xx', inplace=True, axis=1)
    df['x'] = df['x'].shift(1)
    df.fillna(0, inplace=True)
    return df

# 1、在高级别k线的x列上，取出所有值为1的行，得到相应的trade_date作为索引组
# 2、将trade_date索引组应用在低级别k线的trade_date上。在低级别k线上的trade_date列，从索引组元素开始，经过一个高级别周期后，该索引组元素结束
# 3、在低级别周期上，从索引组某个元素开始，经一高级别周期该元素对应的结束，在该周期内判断低级别k线是否有出现交易信号，如果出现则重复至下一个低级别周期


def fuc_digui(a, b, k):  # 索引扩增函数，参数a为索引，b为每次扩增增加步数，k为扩增次数
    if b == 1:
        return a | {i+k for i in a}
    else:
        return fuc_digui(fuc_digui(a, b-1, k), 1, k)


def iterator_macd(*df):
    # 将高级别k线有信号索引经过fuc_digui扩增后得到索引1，然后用低级别周期上有信号的索引2和高级别周期上扩增后索引1进行与操作
    # 再将处理过的低级别周期索引，经过fuc_digui处理后放在次低级别周期上，再重复并操作

    # 该函数接受元组列表，元组中第一元素为dataframe格式周期k线第二元素为k线单周期中15分钟k的数量
    print(len(df))

    for i in range(len(df)-1):
        v_a = df[i]  # v_a为第一个较高周期
        v_b = df[i+1]  # v_b为第二个较低周期
        if i == 0:  # 第一次运行时，获取高级别周期中['x']不为0的索引
            djsy1 = fuc_digui(
                set(v_a[0].loc[v_a[0]['x'] == 1].index), v_a[1]/v_b[1], v_b[1])  # djsy1为将高级做多信号索引扩增后的索引1
            djsy2 = fuc_digui(
                set(v_a[0].loc[v_a[0]['x'] == -1].index), v_a[1]/v_b[1], v_b[1])  # djsy2为将高级做空信号索引扩增后的索引1
        else:
            # 非第一次运行，从上次运行结果的dysj扩增得到新低级别索引1

            # djsy1为将高级做多信号索引扩增后的索引1
            djsy1 = fuc_digui(djsy1, v_a[1]/v_b[1], v_b[1])
            # djsy2为将高级做空信号索引扩增后的索引1
            djsy2 = fuc_digui(djsy2, v_a[1]/v_b[1], v_b[1])
        v_d1 = (v_b[0]['x'] == 1)  # 低级别做多索引2
        v_d2 = (v_b[0]['x'] == -1)  # 低级别做空索引2

        v_c1 = []  # 低级别做多索引1
        v_c2 = []  # 低级别做空索引1 
        
        #通过循环创建list给索引1中不存在的值对应为true，否则反之
        for j in range(len(v_b[0])):
            if j in djsy1:
                v_c1.append(True)
            else:
                v_c1.append(False)
            if j in djsy2:
                v_c2.append(True)
            else:
                v_c2.append(False)
        v_c1 = np.array(v_c1)
        v_c2 = np.array(v_c2)  # 将索引1对应list改为array，方便与操作

        # v_b[0].loc[v_b[0]['x'] != 0, 'x']得到低级别周期上有信号的列，v_b[0].loc[list(djsy), 'x']得到索引1扩增的列
        # 提取索引1和索引2在低级周期上的与操作得到的index转为set 作为索引3
        djyxsy1 = set(v_b[0].loc[v_c1 & v_d1].index)
        djyxsy2 = set(v_b[0].loc[v_c2 & v_d2].index)
        djsy1 = djyxsy1
        djsy2 = djyxsy2  # 将索引3给与索引1在下个循环使用
        print(i)
    return (list(djyxsy1), list(djyxsy2))  # 循环结束后以列表输出索引3
