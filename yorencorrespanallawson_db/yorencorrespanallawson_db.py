# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 10:25:35 2017

@author: Yixiang Zhang
"""

import numpy as np
import pandas as pd

def commodity_sell_report(large_name,begindate=None,enddate=None,region_block_code=None,cursor=None):
    sql1 = '''
SELECT c.COMMODITY_CD, c.COMMODITY_NAME, c.LARGE_NAME, c.MIDDLE_NAME, c.SMALL_NAME, c.SELL_DATE_BEGIN, c.SELL_PRICE, SUM(b.sells_count), COUNT(DISTINCT a.USER_ID)

FROM yoren_new_pos_log a

INNER JOIN t_pos_purchase_commodity_log b 
USING (`shop_id`,`purchase_date`,`pos_no`,`deal_time`,`serial_number`)

LEFT JOIN yoren_commodity_mst c USING (COMMODITY_ID)
LEFT JOIN t_user d ON a.USER_ID=d.user_id

WHERE a.PURCHASE_DATE BETWEEN "%s" AND "%s"
AND a.REGION_BLOCK_CODE = "%s"
AND b.region_block_code = "%s"
AND c.REGION_BLOCK_CODE = "%s"
AND d.del_flg = 0
AND c.LARGE_NAME LIKE '%s'

GROUP BY c.COMMODITY_CD
ORDER BY c.MIDDLE_NAME,c.SMALL_NAME,SUM(sells_count) DESC
;''' % (begindate,enddate,region_block_code,region_block_code,region_block_code,large_name)


    try:
        cursor.execute(sql1)
        results = cursor.fetchall()
        df1 = pd.DataFrame({'commodity_cd':[],'commodity_name':[], 'large_name':[],'middle_name':[],'small_name':[],'sells_count':[],'user_count':[]})
        k = 0
        for row in results:
            df1.loc[k]={'commodity_cd':row[0],'commodity_name':row[1],'large_name':row[2],'middle_name':row[3],'small_name':row[4],'sells_count':row[7],'user_count':row[8]}
            k = k + 1
        print('Step 1 is successful!')
        
    except:
        print('Step 1 failed!')
        
    # 以上第一次try是获取基本信息
    
    try:
        df1['group_rank']=df1['sells_count'].groupby(df1['small_name']).rank(method='max',ascending=False)
        
        k = 0
        for index, row in df1.iterrows():
            if row['group_rank'] <= 9:
                df1.loc[k,['group_rank']]='0'+str(int(row['group_rank']))
            else:
                df1.loc[k,['group_rank']]=str(int(row['group_rank']))
            k = k + 1
        df1['SKU(name)']=df1.small_name + '_' + df1.group_rank
        
        print('Step 2 is successful!')
        return df1
    
    except:
        print('Step 2 failed!')
        
    # 以上第二次try是添加了各个小分类中的销量排名，并添加了一列叫“SKU(name)”
# 以上第一个函数是获取各个商品的销量信息


def record_for_CA(large_name,begindate=None,enddate=None,region_block_code=None,cursor=None):    
    sql2 = '''
SELECT
c.COMMODITY_CD,
c.LARGE_NAME,
c.MIDDLE_NAME,
c.SMALL_NAME,
b.sells_count,
a.USER_ID,
  CASE WHEN d.gender=1 THEN 'M'
       WHEN d.gender=2 THEN 'F' END gender,
  YEAR(CURDATE())-YEAR(d.birthday) AS age,
  CASE WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=10 and (YEAR(CURDATE())-YEAR(d.birthday)) <=19) THEN '10-19'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=20 and (YEAR(CURDATE())-YEAR(d.birthday)) <=24) THEN '20-24'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=25 and (YEAR(CURDATE())-YEAR(d.birthday)) <=29) THEN '25-29'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=30 and (YEAR(CURDATE())-YEAR(d.birthday)) <=34) THEN '30-34'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=35 and (YEAR(CURDATE())-YEAR(d.birthday)) <=39) THEN '35-39'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=40 and (YEAR(CURDATE())-YEAR(d.birthday)) <=44) THEN '40-44'
       WHEN ((YEAR(CURDATE())-YEAR(d.birthday)) >=45 and (YEAR(CURDATE())-YEAR(d.birthday)) <=74) THEN '45-74' END age_group
  
FROM
    yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b USING (
	`shop_id`,
	`purchase_date`,
	`pos_no`,
	`deal_time`,
	`serial_number`
)
LEFT JOIN yoren_commodity_mst c USING (COMMODITY_ID)
LEFT JOIN t_user d ON a.USER_ID=d.user_id

WHERE
    a.PURCHASE_DATE BETWEEN "%s" AND "%s"
AND b.purchase_date BETWEEN "%s" AND "%s"
AND a.REGION_BLOCK_CODE = "%s"
AND b.region_block_code = "%s"
AND c.REGION_BLOCK_CODE = "%s"
AND d.del_flg = 0
AND c.LARGE_NAME LIKE '%s'
AND (YEAR(CURDATE())-YEAR(d.birthday))>=10 AND (YEAR(CURDATE())-YEAR(d.birthday))<=74

ORDER BY
    c.COMMODITY_CD
;''' % (begindate,enddate,begindate,enddate,region_block_code,region_block_code,region_block_code,large_name)
# 以上查询语句用于获取单个SKU与单个User之间一一对应的关系，每一行为一个SKU和一个User之间的组合

    try:
        cursor.execute(sql2)
        results = cursor.fetchall()
        df2 = pd.DataFrame({'commodity_cd':[],'gender':[], 'age_group':[]})
        k = 0
        for row in results:
            df2.loc[k]={'commodity_cd':row[0],'gender':row[6],'age_group':row[8]}  
            k = k + 1
        
        df2['gender&age'] = df2.gender + '_' + df2.age_group
        # 注意：上句不能写df2.gender_age，这样会认为df2.gender_age不是df2的列，而是自己独立的DataFrame
        
        df3 = pd.DataFrame({'commodity_cd':[],'gender&age':[]})
        df3 = df2[['commodity_cd','gender&age']]
        print('Step 3 is successful!')
        return df3
    
    except:
        print('Step 3 failed!')

# 以上第二个函数是获取需要进行对应分析的两列：一列是各SKU的编号，一列是性别与年龄段的组合
# 每行是一个SKU和一个User之间的组合


def CA(dataframe):

    df_dummies=pd.get_dummies(dataframe)
    
    import mca
    mca_ben = mca.MCA(df_dummies, ncols=2)
    fs = mca_ben.fs_c(N=2).T # fs: Factor score

    
    plotdata = pd.DataFrame({'Factor1':fs[0],'Factor2':fs[1],'levelnames':df_dummies.columns})
    
    plotdata.insert(3,'Variable',np.empty(len(plotdata)))
    plotdata.insert(4,'hue',np.empty(len(plotdata)))
    plotdata.insert(5,'SKU(name)',np.empty(len(plotdata)))
    # 或plotdata_new = pd.DataFrame({'Factor1':plotdata.Factor1,'Factor2':plotdata.Factor2,'levelnames':plotdata.levelnames,'Variable':np.empty(len(plotdata))})
    
    k = 0
    for index, row in plotdata.iterrows():
        plotdata.loc[k,['Variable']]= row['levelnames'].split('_')[0]
        plotdata.loc[k,['hue']]= row['levelnames'].split('_')[1]
        plotdata.loc[k,['SKU(name)']]= row['levelnames'].split('_')[1] + '_' + row['levelnames'].split('_')[2]
        k = k + 1

    
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set(color_codes=True)

    plt.rcParams['font.sans-serif']=['SimHei']
    plt.rcParams['axes.unicode_minus']=False

    small_name_num=(plotdata['hue'].groupby(plotdata['hue']).count().size)-2
    
    ii = 65
    list_letter = []
    for num in range(0,small_name_num):
        list_letter = list_letter + [chr(ii)]
        ii = ii + 1
    
    list_letter = ['F','M'] + list_letter
    smallname_to_letter=dict(zip(plotdata['hue'].groupby(plotdata['hue']).count().index,list_letter))
    
    plotdata['letter']=plotdata['hue'].map(smallname_to_letter)
    
    plotdata.insert(7,'SKU(letter)',np.empty(len(plotdata)))
    
    k = 0
    for index, row in plotdata.iterrows():
        plotdata.loc[k,['SKU(letter)']]= row['letter'] + '_' + row['levelnames'].split('_')[2]
        k = k + 1
    
    
    sns.lmplot(x="Factor1", y="Factor2", hue="hue", data=plotdata, fit_reg=False,
               markers=["^", "^"]+["o"]*small_name_num, palette="Set1");
    labels = plotdata['SKU(letter)']
    for label, x, y in zip(labels, plotdata.Factor1, plotdata.Factor2):
        plt.annotate(
            label,
            xy=(x, y), xytext=(-5, 5),
            textcoords='offset points', ha='right', va='bottom',
            bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
            fontsize=5,
            arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))
    plt.show()
    return plotdata
