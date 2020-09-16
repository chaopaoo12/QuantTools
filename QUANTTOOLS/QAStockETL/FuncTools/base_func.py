import time

def time_this_function(func):
    #作为装饰器使用，返回函数执行需要花费的时间
    def inner(*args,**kwargs):
        start=time.time()
        result=func(*args,**kwargs)
        end=time.time()
        print(func.__name__,end-start)
        return result
    return inner


def mkdir(path):
    # 引入模块
    import os

    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        try:
            os.makedirs(path)

            print(path + ' 创建成功')
            return True
        except:
            return False
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')
        return True