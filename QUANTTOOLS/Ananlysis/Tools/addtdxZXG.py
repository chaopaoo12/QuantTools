
#通达信股票列表转自选股数据转
path = 'E:\\Program Files\\new_tdx\\T0002\\blocknew\\ZXG.blk'


def putzxg(code):
    if code[0:2] == '60' and len(code) == 6:
        code1 = ''+chr(10)+'1' + code
    elif code[0:3] == '688':
        code1 = ''+chr(10)+'1'+code
    elif code[0:3] in ['000','002','300'] and len(code) == 6:
        code1 = ''+chr(10)+'0'+code
    else:
        code1 = code
    return code1

def putzxgfile(l,path=path):
    with open(path,'w') as f:
        f.write(''+chr(10)+'1000001')
        f.write(''+chr(10)+'0399006')
        f.write(''+chr(10)+'0399001')
        for code in l:
            s=putzxg(code)
            f.write(s)