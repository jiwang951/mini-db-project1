# run_tests.py

from mini_db import MiniDB

def run_tests():
    db = MiniDB()

    # 创建表
    try:
        db.create_table("employees", ["id", "name", "age"])
    except ValueError as e:
        print(e)

    # 插入数据
    try:
        db.insert_into("employees", [1, "Alice", 30])
        db.insert_into("employees", [2, "Bob", 25])
        db.insert_into("employees", [3, "Charlie", 40])
    except ValueError as e:
        print(e)

    # 创建索引
    try:
        db.create_index("employees", "age")
    except ValueError as e:
        print(e)

    # 创建唯一索引
    try:
        db.create_unique_index("employees", "id")
    except ValueError as e:
        print(e)

    # 插入重复数据，测试唯一索引
    try:
        db.insert_into("employees", [1, "David", 35])
    except ValueError as e:
        print(e)

    # 查询数据
    try:
        db.select_from("employees", ["name", "age"])
        db.select_from("employees", ["*"], where=("age", 30))
        db.select_from("employees", ["*"], order_by="age")
    except ValueError as e:
        print(e)

if __name__ == "__main__":
    run_tests()
