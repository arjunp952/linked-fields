import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import re
import os

sql = ""
fileList = []
for file in os.listdir('.'):
    if file.endswith('.txt'):
        fileList.append(file)

for doc in fileList:
    to_find = open(doc,"r")
    for line in to_find:
        sql += line      


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    #i = 0
    for item in parsed.tokens:
        print(item)
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            #elif item.ttype is Keyword:
                #raise StopIteration
            else:
                #FIXME: The problem child (for Source tables)
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
        # TODO
        #elif item.value.upper() == 'INNER JOIN':
        #    print("Hi")
        #i += 1

def parse_tokens(parsed):
    tokenList=[]
    intoIndex = 0
    for item in parsed.tokens:
        tokenList.append(item)
    for word in tokenList:
        if (word.value.upper()=="INTO"):
            intoIndex = tokenList.index(word)
        elif(word.value.upper()=="VIEW"):
            intoIndex = tokenList.index(word)
    if intoIndex >= 0:
        tableIndex = intoIndex+2
        return tokenList[tableIndex]
    else:
        return ' '

def get_from(parsed):
    tokenList=[]
    intoIndex = 0
    for item in parsed.tokens:
        tokenList.append(item)
    for word in tokenList:
        if (word.value.upper()=="FROM"):
            intoIndex = tokenList.index(word)
    if intoIndex >= 0:
        tableIndex = intoIndex+2
        return tokenList[tableIndex]
    else:
        return ' '    

def get_columns(parsed):
    tokenList=[]
    intoIndex = 0
    for item in parsed.tokens:
        tokenList.append(item)
    for word in tokenList:
        if (word.value.upper()=="SELECT"):
            intoIndex = tokenList.index(word)
    tableIndex = intoIndex+2
    return tokenList[tableIndex]

def get_joins(parsed):
    tokenList=[]
    joinList=[]
    intoIndex = 0
    for item in parsed.tokens:
        tokenList.append(item)
    for word in tokenList:
        # TODO: Not sure if working properly
        if (word.value.upper()=="INNER JOIN") or (word.value.upper()=="LEFT OUTER JOIN") or (word.value.upper()=="RIGHT OUTER JOIN") or (word.value.upper()=="CROSS JOIN") or (word.value.upper()=="LEFT JOIN") or (word.value.upper()=="RIGHT JOIN") or (word.value.upper()=="FULL JOIN"):
            intoIndex = tokenList.index(word)
            tableIndex = intoIndex+2
            joinList.append(tokenList[tableIndex])
    return joinList    
            

def extract_table_identifiers(token_stream):    
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value

            
def extract_fromtables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))

def return_alias(query):
    query_alias = []
    alias_error = "===== Error - No alias recognized."
    for item in query:
        if item.has_alias():
            query_alias.append(item.get_alias())
        else:
            print(alias_error)
    for alias in query_alias:
        return alias

if __name__ == '__main__':
    #sql = """ CREATE VIEW vw_table_one AS SELECT id, name as first, age from (select uid, uname, uage from vw_table_three) where vw_table_one.id = vw_table_three.id group by age order by name """
    #print(sqlparse.parse(sql)[0].tokens)
    #sql = """CREATE view [dbo].[vw_p2k_vf_compare_pr8949] AS select apples AS redfruit from fruits"""
    #sql = """select a as b from (select id from (select myid from e)) """
    #sql = """SELECT a AS b, c as d into receiver as rcvr from e"""
    #sql = """CREATE VIEW [dbo].[vw_wasp_smr_history_rept] AS SELECT d.AppName AS [App Name], d.Item_Name AS [Item Name],d.Data_Date FROM (SELECT AppName,Item_Name,Data_Date FROM wasp_smr_monthly_history) d"""
    sql = """CREATE VIEW dbo.worldcity AS SELECT e.emp_name AS myempname,w.city FROM employees e INNER JOIN world w ON e.city = w.city LEFT OUTER JOIN continent c on c.country = w.country"""


def getinfo():
    sourced = get_from(sqlparse.parse(sql)[0])
    print('Source tables:',sourced)
    
    joinExtract = get_joins(sqlparse.parse(sql)[0])
    for i in joinExtract:
        print('Join tables:',i)
    
    parsed = parse_tokens(sqlparse.parse(sql)[0])
    print('Target tables:',parsed)

    columnExtract = get_columns(sqlparse.parse(sql)[0])
    print('Columns:',columnExtract)

getinfo()

    

