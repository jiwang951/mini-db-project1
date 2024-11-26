# mini_db.py

class MiniDB:
    def __init__(self):
        self.tables = {}
        self.indexes = {}
        self.unique_indexes = {}

    # Basic - 创建表
    def create_table(self, table_name, columns):
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists.")
        self.tables[table_name] = {
            "columns": columns,
            "data": []
        }
        print(f"Table '{table_name}' created with columns: {columns}")

    # Basic - 插入数据
    def insert_into(self, table_name, values):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        if len(values) != len(table["columns"]):
            raise ValueError("Column count does not match value count.")

        # 处理 UNIQUE 约束
        for column in self.unique_indexes.get(table_name, []):
            col_index = table["columns"].index(column)
            if any(row[col_index] == values[col_index] for row in table["data"]):
                raise ValueError(f"Duplicate entry for unique column '{column}'.")

        table["data"].append(values)

        # 更新索引
        for (t_name, col_name), index in self.indexes.items():
            if t_name == table_name:
                column_index = table["columns"].index(col_name)
                value = values[column_index]
                if value not in index:
                    index[value] = []
                index[value].append(len(table["data"]) - 1)

        print(f"Inserted values {values} into table '{table_name}'.")

    # Basic - 创建索引
    def create_index(self, table_name, column_name):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        if column_name not in self.tables[table_name]["columns"]:
            raise ValueError(f"Column {column_name} does not exist in table {table_name}.")

        # 创建索引
        index = {}
        column_index = self.tables[table_name]["columns"].index(column_name)

        # 创建索引
        for i, row in enumerate(self.tables[table_name]["data"]):
            value = row[column_index]
            if value not in index:
                index[value] = []
            index[value].append(i)

        self.indexes[(table_name, column_name)] = index
        print(f"Index created on column '{column_name}' of table '{table_name}'.")

    # Unique - 创建唯一索引
    def create_unique_index(self, table_name, column_name):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        if column_name not in self.tables[table_name]["columns"]:
            raise ValueError(f"Column {column_name} does not exist in table {table_name}.")

        if table_name not in self.unique_indexes:
            self.unique_indexes[table_name] = []
        self.unique_indexes[table_name].append(column_name)
        print(f"Unique index created on column '{column_name}' of table '{table_name}'.")

    # Basic - 查询数据
    def select_from(self, table_name, columns, where=None, group_by=None, order_by=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]

        if columns == ["*"]:
            columns = table["columns"]

        indices = [table["columns"].index(col) for col in columns]

        # Apply WHERE condition if provided
        selected_rows = []
        if where:
            column_name, value = where
            if (table_name, column_name) in self.indexes:
                row_indices = self.indexes[(table_name, column_name)].get(value, [])
                selected_rows = [table["data"][i] for i in row_indices]
            else:
                col_index = table["columns"].index(column_name)
                selected_rows = [row for row in table["data"] if row[col_index] == value]
        else:
            selected_rows = table["data"]

        # Apply GROUP BY if provided
        if group_by:
            group_index = table["columns"].index(group_by)
            groups = {}
            for row in selected_rows:
                key = row[group_index]
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)
            selected_rows = [group for group in groups.values()]

        # Apply ORDER BY if provided
        if order_by:
            order_index = table["columns"].index(order_by)
            selected_rows = sorted(selected_rows, key=lambda x: x[order_index])

        result = []
        for row in selected_rows:
            selected_row = [row[i] for i in indices]
            result.append(selected_row)

        print(f"Selected rows from '{table_name}': {result}")
        return result
