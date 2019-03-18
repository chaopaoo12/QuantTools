from QUANTTOOLS.FactorTools import SingelePeriodFactorTest
import pandas as pd
import numpy as np

def MultiPeriodFactorTest(factor,DateStart,DateEnd,if_reciprocal,if_neutral_industry=True, if_neutral_mktcap=True):

    # 获取交易日序列
    BargainDate = w.tdays(DateStart, DateEnd, "Period=M")
    BargainDate = pd.DataFrame(BargainDate.Data[0],columns = ['date'])
    # 循环调用单期因子测试函数，得到收益率序列，IC序列，t值序列
    result = pd.DataFrame(columns=["DateStart","DateEnd","factor_return","t_values","IC"])
    for i in range(1,BargainDate.shape[0]):
        datebuy = BargainDate.date[i-1]
        datesell = BargainDate.date[i]
        result1 = SingelePeriodFactorTest(factor,datebuy,datesell,if_neutral_industry, if_neutral_mktcap,if_reciprocal)
        result = result.append(result1)

    result.factor_return = result.factor_return

    # 计算均值
    t_mean = result.t_values.mean()
    return_mean =  result.factor_return.mean()
    t_abs_mean = result.t_values.abs().mean()


    # t>0比例
    if_t_0 = pd.DataFrame.mean(result.t_values>0)


    #     IC统计量
    IC_mean = result.IC.mean()
    IC_std = result.IC.std()
    if_IC_0 = pd.DataFrame.mean(result.IC>0)
    if_abs_IC_002 = pd.DataFrame.mean(result.IC.abs()>0.02)


    # 计算ICIR
    ICIR = IC_mean/IC_std

    final = {"因子收益序列t均值":t_mean,
             "因子收益序列均值":return_mean,
             "t>0比例":if_t_0,
             "abs(t)均值":t_abs_mean,
             "IC均值":IC_mean,
             "IC标准差":IC_std,
             "IC>0比例":if_IC_0,
             "abs(IC)>0.02比例":if_abs_IC_002,
             "ICIR":ICIR
             }
    #    '''
    #    作图
    #    '''
    #    X = np.arange(result.shape[0])
    #
    #    # 因子收益
    #    plt.figure()
    #    plt.subplot(221)
    #    plt.bar(X,result.factor_return)
    #    plt.title('factor_return')
    #    plt.subplot(222)
    #    plt.hist(result.factor_return)
    #    plt.title('factor_return')
    #
    #    # t值
    #    plt.subplot(223)
    #    plt.bar(X,abs(result.t_values))
    #    plt.title('abs(t_value)')
    #
    #
    #    # IC
    #    plt.subplot(224)
    #    plt.bar(X,result.IC)
    #    plt.title('IC')
    #    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    #

    return(final,result)

# 固定顺序的施密特正交化 从左向右依次正交  返回Q为正交因子矩阵
def Schimidt(factors):
    class_mkt = factors[['mkt_cap','classname']]
    factors1 = factors.drop(['mkt_cap','classname'],axis = 1)
    col_name = factors1.columns
    factors1 = factors1.values

    R = np.zeros((factors1.shape[1], factors1.shape[1]))
    Q = np.zeros(factors1.shape)
    for k in range(0, factors1.shape[1]):
        R[k, k] = np.sqrt(np.dot(factors1[:, k], factors1[:, k]))
        Q[:, k] = factors1[:, k]/R[k, k]
        for j in range(k+1, factors1.shape[1]):
            R[k, j] = np.dot(Q[:, k], factors1[:, j])
            factors1[:, j] = factors1[:, j] - R[k, j]*Q[:, k]

    Q = pd.DataFrame(Q,columns = col_name,index = factors.index)
    Q = pd.concat([Q,class_mkt],axis = 1)
    return Q

# 规范正交
def Canonial(self,factors):
    class_mkt = factors[['mkt_cap','classname']]
    factors1 = factors.drop(['mkt_cap','classname'],axis = 1)
    col_name = factors1.columns
    D,U=np.linalg.eig(np.dot(factors1.T,factors1))
    S = np.dot(U,np.diag(D**(-0.5)))

    Fhat = np.dot(factors1,S)
    Fhat = pd.DataFrame(Fhat,columns = col_name,index = factors.index)
    Fhat = pd.concat([Fhat,class_mkt],axis = 1)

    return Fhat

# 对称正交
def Symmetry(factors):
    class_mkt = factors[['mkt_cap','classname']]
    factors1 = factors.drop(['mkt_cap','classname'],axis = 1)
    col_name = factors1.columns
    D,U=np.linalg.eig(np.dot(factors1.T,factors1))
    S = np.dot(U,np.diag(D**(-0.5)))

    Fhat = np.dot(factors1,S)
    Fhat = np.dot(Fhat,U.T)
    Fhat = pd.DataFrame(Fhat,columns = col_name,index = factors.index)
    Fhat = pd.concat([Fhat,class_mkt],axis = 1)

    return Fhat