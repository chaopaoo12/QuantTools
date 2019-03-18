import pandas as pd
import statsmodels.api as sml
from QUANTTOOLS.FactorTools.base_tools import (standardize_series,
                                               filter_extreme_MAD,filter_extreme_3sigma,filter_extreme_percentile,
                                               neutralization,
                                               get_factor_ic,halflife_ic)

def SingelePeriodFactorTest(factor,DateStart,DateEnd,if_neutral_industry, if_neutral_mktcap,if_reciprocal):

    factor_name = factor['value'][0][0]

    print(factor_name + '单因子测试：' + str(DateStart) + ' -- ' + str(DateEnd))

    #  todo 数据重新获取 整合
    data = getData(factor,DateStart)
    # 是否取倒数
    if if_reciprocal:
        data.iloc[:,0] = 1/data.iloc[:,0]
    # 通过本地数据库计算股票收益率
    stock_list = data.index.tolist()
    stock_list = list(map(lambda x:x.strip('.SZ'),stock_list))
    stock_list = list(map(lambda x:x.strip('.SH'),stock_list))
    stock_list = tuple(stock_list)
    price_s = pd.read_sql("select stockcode,closeprice as price_s \
						   from database\
						   where stockcode in {} and bargaindate='{}'\
						   order by stockcode".format((stock_list),DateStart),stkbase)
    price_s=price_s.set_index('stockcode')
    price_e = pd.read_sql("select stockcode,closeprice as price_e \
						   from database\
						   where stockcode in {} and bargaindate='{}'\
						   order by stockcode".format((stock_list),DateEnd),stkbase)



    price_e=price_e.set_index('stockcode')
    price=pd.merge(price_s,price_e,left_index=True,right_index=True)


    price['rt']=price['price_e']/price['price_s']-1
    # 标准化
    factor_all= neutralization(data,if_neutral_industry, if_neutral_mktcap)
    factor_all.index.name = 'stockcode'
    factor_all = factor_all.reset_index()
    factor_all['stockcode'] = factor_all['stockcode'].apply(lambda x:x.strip('.SZ|.SH'))
    factor_all = factor_all.set_index('stockcode')
    Alldata = pd.merge(factor_all,price,left_index=True,right_index=True,how = 'left')




    y = Alldata[['rt']]
    X = factor_all
    X['Intercept'] = 1

    model = sml.OLS(y,X)
    result=model.fit()


    # 计算IC,IR
    IC = get_factor_ic(factor_all[factor_name], y, 'Spearman')

    print(str(DateStart) + "——" +str(DateEnd)+","+factor_name + '因子收益为:'+ str(round(result.params[0],4)*100) +'%')

    return_t=pd.DataFrame([[DateStart,DateEnd,result.params[0],result.tvalues[0],IC]],
                          columns=["DateStart","DateEnd","factor_return","t_values","IC"])

    return(return_t)