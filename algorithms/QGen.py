import re
import pandas as pd


def minus_query(srcElem,srcSchema):
    query = 'Select '
    # made to contain a list of all column names
    tc_list = []
    tc_string = ''
    targetSchema = srcElem[0][-1]
    for line in srcElem:
        if len(line) == 3:
            column_name = line[1]
            if column_name == 'file_name':
                break
            tc_list.append(column_name)
            data_type = line[2]
            if data_type.endswith(','):
                data_type = data_type[0:len(data_type)-1]
            if 'varchar' in data_type:
                query = query + "\ncase when " + column_name + " ='' then null else " + column_name + " end,"
            else:
                query = query + "\ncase when " + column_name + " ='' then null else cast(" + column_name + " as " + data_type + ") end,"
    query2_src = query[0:len(query)-1]+"\nfrom " + srcSchema + ";"
    for i in range(0, len(tc_list)):
        tc_string = tc_string+"\n"+tc_list[i]+','
    tc_string = tc_string[0:len(tc_string)-1]
    query = query[0:len(query)-1] + "\nfrom " + srcSchema + "\nminus select" + tc_string + "\nfrom " + targetSchema + ";"
    query2 = "Select" + tc_string + "\nfrom " + targetSchema + "\nminus\n" + query2_src
    m_query = [query, query2]
    return m_query


def generator(source_list, sc):
    #source_list has the DDL as a list of lines from 1 individual cell
    src_elements = [None] * len(source_list)
    for j in range(len(source_list)):
        src_elements[j] = re.split('\s+', source_list[j])
    #extracting table name from provided DDL
    table_name = (src_elements[0][-1].split('.'))[-1]
    sc_table = sc+'.'+table_name
    temp = minus_query(src_elements, sc_table)
    return temp

if __name__ == "__main__":
    data = pd.read_excel(r'InputQGen.xlsx',"Sheet1")
    df = pd.DataFrame(data, columns=['DDL'])
    srcList = []
    generated_query = []
    srctrg = []
    trgmsrc = []
    srcSchema = (pd.DataFrame(data, columns=['Source Schema'])).iat[0, 0]
    #rwdex_raw_navigating_cancer_load_helper
    for i in range(len(data)):
        # the cell at index i is appended to srcList
        srcList.append(df.iloc[i,0])
        # to split the individual cell content into individual lines
        srcSplitList = srcList[i].splitlines()
        generated_query.append(generator(srcSplitList, srcSchema))
        srctrg.append(generated_query[i][0])
        trgmsrc.append(generated_query[i][1])

    data["Minus Query(Source - Target)"] = srctrg
    data["Minus Query(Target - Source)"] = trgmsrc
    data.to_excel(r'QGen.xlsx', index=False)
    print("All Queries have been generated and placed in file = 'QGen' in same directory")






