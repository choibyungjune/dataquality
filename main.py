import os
from csv_handler import csv_handler
from sql_handler import sql_handler

if __name__ == '__main__':
    # input : "csv" or "db"
    # debug_tool = "db"
    while True:
        debug_tool = input('csv or db 어떤 것을 사용하시겠습니까?')
        if debug_tool == "csv":
            csv_handler()
            break
        elif debug_tool == "db" :
            sql_handler()
            break
        else:
            print("다시 입력해주십시오")

























os.system("pause")