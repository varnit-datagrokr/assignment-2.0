import json

def extract(filename):
    sql_query = ''
    with open(filename,'r') as rf:
        sql_query = rf.read()

    statements = sql_query.split(";")
    result_list = []
    statement_id = 1
    for statement in statements:
        statement_type = statement.strip().split(' ')[0].strip()
        local_dict = {
            "statement_id": statement_id,
            "statement_type": statement_type,
            "target_table_name": []
        }
        lines = statement.splitlines()
        statement_id += 1
        for line in lines:
            for type in ["FROM","JOIN"]:
                if line.find(type) != -1:
                    table_name = line.strip().split(type)[1]
                    if table_name.strip().find(" ") != -1:
                        table_name
                    if table_name.find(".") != -1:
                        table_name = table_name.split(".")[0]

                    local_dict["target_table_name"].append({
                        "type": type,
                        "name": table_name,
                        "full query": line
                    })

        result_list.append(local_dict)

    with open(filename.replace('sql','json'),'w') as wf:
        json.dump(result_list,wf,indent=4)



if __name__ == '__main__':
    extract('database/sample_stored_procedure.sql')
