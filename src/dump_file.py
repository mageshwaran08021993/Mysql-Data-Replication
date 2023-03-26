with open("D:\MYSQL\data\insert_query_tbl_properties.sql", "r") as dump_file:
    data = dump_file.read()

for line in data:
    each_statment = line.split("),")
    with open("D:\MYSQL\data\insert_query_tbl_properties_test.sql", "a") as file:
        for s in each_statment:
            file.write(s+"\n")

    break