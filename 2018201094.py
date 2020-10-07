import csv
import sys
import sqlparse
import sys
import re
import copy
#################################################   reading metadata ##################################################

def read_tables(filename):
    table=[]
    filename='files/'+filename+'.csv'
    with open(filename, 'r') as csvFile:
        reader = csv.reader(csvFile)
        for row in reader:
            table.append(row)
    csvFile.close()
    return table


def check(list,value):
    try:
        indx=table.index(x)
        result=True
    except:
        result=False
    return result

#################################################   cartesian product ##################################################

def cartesian_product(frm_,select):

    metadata,col_tab=read_metadata()
    column_list=[]
    for x in frm_:
        for y in metadata:
            if y[0]==x:
                for z in range(1,len(y)):
                    temp=x+'.'+y[z]
                    column_list.append(temp)

    # print(column_list)
    for x in frm_:
        avail=False
        for y in metadata:
            if x==y[0]:
                avail=True
                break
        if avail==False:
            print("ERROR !!! table not found")
            exit(0)
            break
        
    new_data=read_tables(frm_[0])
    # frm_.remove(frm_[0])
    if len(frm_)==1:
        return new_data,column_list
    else:
        for x in range(1,len(frm_)):
            empty1=[]
            for y in new_data:
                p=read_tables(frm_[x])
                # empty1=[]
                for z in p:
                    temp=y
                    temp=temp+z
                    empty1.append(temp)
            new_data=empty1                
        return new_data,column_list

#################################################   reading metadata ##################################################

def read_metadata():
    table_list = []
    start = '<begin_table>'
    end = '<end_table>'
    lines = open('files/metadata.txt', 'r')
    file_data=[]
    for line in lines:
         file_data.append(line.strip())
    lines.close()
    # print (file_data)
    tables=[]
    for i in range(len(file_data)):
        if file_data[i]==start:
            table=[]
            for x in range(i+1,len(file_data)):
                if file_data[x]!=end:
                    table.append(file_data[x])
                else:
                    break
            tables.append(table)

    col_tab={}
    for x in tables:
        for y in range(1,len(x)):
            col_tab.setdefault(x[y], []).append(x[0])

    # print(col_tab)


    # print(tables)
    return tables,col_tab

#################################################   query parsing ##################################################

def query_parsing(query):
  
    lst=query.split(' ')
    # for x in query:
    #     lst.append(x)
    #     # print(str(x))

    # print(lst)

    lst = list(filter(None, lst))
    # print(lst)

    sl_key=False
    fr_key=False
    # wr_key='where'
    select=[]
    frm=[]
    where=[]

    # print(lst)
    for x in lst:
        # print(x)
        if x.lower()=='select':
            sl_key=True
        if x.lower()=='from':
            fr_key=True

    # print(sl_key)
    if sl_key==False :
        print("ERROR !!! No select in query")
        exit(0)
    if fr_key==False :
        print("ERROR !!! No from in query")
        exit(0)
        
    lst.remove(lst[0])
    # print(lst)
    distinct=lst[0]
    if distinct.lower()=='distinct':
        dist=True
        lst.remove(lst[0])
    else:
        dist=False
    

    for x in range(len(lst)):
        if (lst[x].lower())!='from':
            select.append(lst[x])
            lst[x]=""
        else:
            lst[x]=""
            break
    # print(select)

    lst = list(filter(None, lst))
    # print(select)
    # print()
    # print(lst)    

    for x in range(len(lst)):
        if lst[x].lower()!="where":
            frm.append(lst[x])
            lst[x]=""
        else:
            lst[x]=""
            break

    lst = list(filter(None, lst))
    # print(frm)
    # print()
    # print(lst)    

    for x in range(len(lst)):
        where.append(lst[x])

    # print(where)
    # print()
    # print(lst)
    return select,frm,where,dist

################################################   select any queries  ################################################

def select_any(select,frm,is_distinct,data,column_list,any_list):

    metadata,col_tab=read_metadata()
    columns=[]
    #{'A': ['table1'], 'B': ['table1', 'table2'], 'C': ['table1'], 'D': ['table2']}
    #select A,B from table1,table2;
    # print(select,"select")
    # print(data)
    for x in select:
        tempo=x.split('.')
        if len(tempo)==1:
            count=0
            for y in frm:
                try:
                    for z in col_tab[x]:
                        if z==y:
                            temp_col=y+'.'+x
                            count+=1
                        if count>1:
                            print("ERROR !!! Ambiguity in column")
                            exit(0)
                except:
                    print("ERROR !!! Incorrect column")
                    exit(0)
            columns.append(temp_col)
        else:
            columns.append(x)
            
    # print(columns,"hello honey")
    index_list=[]
    for x in columns:
        try:
            indx=column_list.index(x)
        except:
            print("ERROR !!! Incorrect column")
            exit(0)
        index_list.append(indx)
    # print(column_list)
    # print(index_list)
    # print(columns,"columnss")

    for i in range(len(columns)):
        if i==len(columns)-1:
            print(columns[i])
        else:
            print(columns[i],end=",")


    if is_distinct==False:    
        for x in data:
            for y in range(len(index_list)):
                if y==len(index_list)-1:
                    print(x[index_list[y]])
                else:
                    print(x[y],end=',')
    else :
        unique_data=[]
        for x in data: 
            if x not in unique_data: 
                unique_data.append(x) 
        
        for x in unique_data:
            for y in range(len(index_list)):
                if y==len(index_list)-1:
                    print(x[index_list[y]])
                else:
                    print(x[y],end=',')
    


################################################   select all queries  ################################################

def select_all(frm,is_distinct,data,column_list):

    # if len(frm)==1:
        if is_distinct==False:
            
            for i in range(len(column_list)):
                if i==len(column_list)-1:
                    print(column_list[i])
                else:
                    print(column_list[i],end=",")
            # data=read_tables(table[0])
            for i in (data):
                for j in range(len(i)):
                    if j==len(i)-1:
                        print(i[j])
                    else:
                        print(i[j],end=",")
        else:
            
            # new_table=[]
            
            for i in range(len(column_list)):
                if i==len(column_list)-1:
                    print(column_list[i])
                else:
                    print(column_list[i],end=",")
            # data=read_tables(table[0])
            
            unique_data=[]
            for x in data: 
                if x not in unique_data: 
                    unique_data.append(x) 

            for i in (unique_data):
                for j in range(len(i)):
                    if j==len(i)-1:
                        print(i[j])
                    else:
                        print(i[j],end=",")


################################################    aggregate func    ################################################

def select_aggregate(frm,func,col,data,new_columns):
    
    metadata,col_tab=read_metadata()

    # for x in metadata:
    #     if x[0]==frm[0]:
    #         table=x
    #         break
    # # print(table)
    # try:
    #     indx=table.index(col)
    #     indx=indx-1
    # except:
    #     print("ERROR !!! Incorrect column") 
    #     exit(0)
    # # print(indx)
    # # data=read_tables(table[0])
    tem0=col.split('.')
    if len(tem0)==1:
        count=0
        for y in frm:
            try:
                for z in col_tab[col]:
                    if z==y:
                        temp_col=y+'.'+col
                        count+=1
                    if count>1:
                        print("ERROR !!! Ambiguity in column in where clause")
                        exit(0)
            except:
                print("ERROR !!! Incorrect column")
                exit(0)
        col=temp_col
            

    indx=new_columns.index(col)
    print(col)

    if func.lower()=='max':
        # print("max")

        new_list=[]
        for x in data:
            new_list.append(int(x[indx]))

        maxi=max(new_list)
        print(maxi)

    elif func.lower()=='min':
        # print("min")

        new_list=[]
        for x in data:
            new_list.append(int(x[indx]))

        mini=min(new_list)
        print(mini)

    elif func.lower()=='sum':
        # print("sum")

        new_list=[]
        for x in data:
            new_list.append(int(x[indx]))

        sum_= sum(new_list)
        print(sum_)

    elif func.lower()=='avg':
        # print("avg")

        new_list=[]
        for x in data:
            new_list.append(int(x[indx]))

        sum_=sum(new_list)
        count=len(new_list)

        avg=sum_/count
        print(avg)

    else:
            print("ERROR !!! incorrect aggregate func")
            exit(0)
################################################    check_aggregate    ################################################

def check_aggregate(select):
    word=select[0]
    front=word[:4]
    end=word[-1]
    # print(word[4:-1])
    # print(front)
    # print(end)
    if (front.lower()=='max('or front.lower()=='min('or front.lower()=='avg('or front.lower()=='sum(') and (end==')'):
        return True,front[:-1],word[4:-1]
    else:
        return False,None,None
######################################################   main   #######################################################

def main(query):

    # query1='select A,table2.B from table1,table2'
    # query0='select * from table1,table2'
    # query2='select  A,B,C from table1,table2'
    # query3='select   A,table1.B,table2.B  from  table1,table2  where  A <= table1.B and table2.B > 100'
    select,frm,where,is_distinct=query_parsing(query)
    # print(select,frm,where,is_distinct)
    if len(select)==0 or len(frm)==0:
        print("ERROR !!! no column name or no table name")
        exit(0)
    new_select=[]
    for i in select:
        temp=i.split(',')
        for j in temp:
            new_select.append(j)
    select=new_select
    select = list(filter(None, select))

    new_frm=[]
    for i in frm:
        temp=i.split(',')
        for j in temp:
            new_frm.append(j)
    frm=new_frm
    frm = list(filter(None, frm))

    new_data,new_columns=cartesian_product(frm,select)

    # for x in new_data:
    #     print(x)
    
    any_list=[]
    if len(frm)>1:
        metadata,col_tab=read_metadata()
        check_list=[]
        for x in frm:
            for y in metadata:
                if y[0]==x:
                    check_list.append(y)

        # print(check_list,"check_list")
        for c in select:
            tempo=c.split('.')
            if len(tempo)==1:
                for d in check_list:
                    for e in range(1,len(d)):
                        # print(c,d[e])
                        if c==d[e]:
                            any_list.append(d[0]+'.'+d[e])
            else:
                any_list.append(c)
        # print(any_list,"anylist")

    if len(where)==0:
        # print(len(frm))
        if len(frm)==1:
            if len(select)==1:
                istrue,func,col=check_aggregate(select)

                if istrue==True:
                    select_aggregate(frm,func,col,new_data,new_columns)
                elif select[0]=='*':
                    select_all(frm,is_distinct,new_data,new_columns)
                else:
                    select_any(select,frm,is_distinct,new_data,new_columns,any_list)
            else:
                select_any(select,frm,is_distinct,new_data,new_columns,any_list)
                
        else :
            # if select[0]=='*':
            #     select_all(frm,is_distinct,new_data,new_columns)
            # else:
            #     select_any(select,frm,is_distinct,new_data,new_columns,any_list)
            if len(select)==1:
                istrue,func,col=check_aggregate(select)

                if istrue==True:
                    select_aggregate(frm,func,col,new_data,new_columns)
                elif select[0]=='*':
                    select_all(frm,is_distinct,new_data,new_columns)
                else:
                    select_any(select,frm,is_distinct,new_data,new_columns,any_list)
            else:
                select_any(select,frm,is_distinct,new_data,new_columns,any_list)

    else:
        # if len(frm)==1:
            is_AND=False
            is_OR=False
            for i in where:
                if i.lower()=='and':
                    is_AND=True
                    break
                if i.lower()=='or':
                    is_OR=True
            
            # print(is_AND,is_OR)
            list_one=[]
            list_two=[]
            count=0
            for i in where:
                # print(i)
                count+=1
                if (i.lower()!='and') and (i.lower()!='or'):
                    list_one.append(i)
                else:
                    break
            
            # print(count)          
            for i in range(count,len(where)):
                list_two.append(where[i])
            
            for t in range(1,len(list_one)):
                list_one[0]=list_one[0]+list_one[t]

            for t in range(1,len(list_two)):
                list_two[0]=list_two[0]+list_two[t]
            
            symbol=None
            if is_AND==True:
                symbol='and'
            elif is_OR==True:
                symbol='or'

            # print(list_one,list_two,symbol,"lists")
            result=[]
            if symbol!=None:
                
                if list_one==0 or list_two==0:
                    print("ERROR !!! mistake in where clause")
                    exit(0)
                else:
                    hell1=False
                    hell2=False
                    if symbol=='and':

                        if len(list_one[0].split('<='))!=1:
                            cond_col=list_one[0].split('<=')[0]
                            cond='<='
                            cond_value=list_one[0].split('<=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('>='))!=1:
                            cond_col=list_one[0].split('>=')[0]
                            cond='>='
                            cond_value=list_one[0].split('>=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('='))!=1:
                            cond_col=list_one[0].split('=')[0]
                            cond='='
                            cond_value=list_one[0].split('=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('<'))!=1:
                            cond_col=list_one[0].split('<')[0]
                            cond='<'
                            cond_value=list_one[0].split('<')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('>'))!=1:
                            cond_col=list_one[0].split('>')[0]
                            cond='>'
                            cond_value=list_one[0].split('>')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        # print(cond_col,cond,cond_value)
                        metadata,col_tab=read_metadata()
                        
                        tem0=cond_col.split('.')
                        if len(tem0)==1:
                            count=0
                            for y in frm:
                                try:
                                    for z in col_tab[cond_col]:
                                        if z==y:
                                            temp_col=y+'.'+cond_col
                                            count+=1
                                        if count>1:
                                            print("ERROR !!! Ambiguity in column in where clause")
                                            exit(0)
                                except:
                                    print("ERROR !!! Incorrect column in where clause")
                                    exit(0)
                            cond_col=temp_col

                        if hell1==True:
                            tem0=cond_value.split('.')
                            if len(tem0)==1:
                                count=0
                                for y in frm:
                                    try:
                                        for z in col_tab[cond_value]:
                                            if z==y:
                                                temp_col=y+'.'+cond_value
                                                count+=1
                                            if count>1:
                                                print("ERROR !!! Ambiguity in column in where clause")
                                                exit(0)
                                    except:
                                        print("ERROR !!! Incorrect column in where clause")
                                        exit(0)
                                cond_value=temp_col
                            
                        try:
                            indexx=new_columns.index(cond_col)
                            if cond_value.isdigit()==False:
                                indexxx=new_columns.index(cond_value)
                        except:
                            print("ERROR !!! Incorrect column in where")
                            exit(0)

                        passing_data=[]
                        if cond=='<=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])<=int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]<=x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='>=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])>=int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]>=x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])==int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]==x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='<':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])<int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]<x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='>':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])>int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]>x[indexxx]:
                                        passing_data.append(x)

                        # print(passing_data)

                        ##########################################

                        if len(list_two[0].split('<='))!=1:
                            cond_col2=list_two[0].split('<=')[0]
                            cond2='<='
                            cond_value2=list_two[0].split('<=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True

                        elif len(list_two[0].split('>='))!=1:
                            cond_col2=list_two[0].split('>=')[0]
                            cond2='>='
                            cond_value2=list_two[0].split('>=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('='))!=1:
                            cond_col2=list_two[0].split('=')[0]
                            cond2='='
                            cond_value2=list_two[0].split('=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('<'))!=1:
                            cond_col2=list_two[0].split('<')[0]
                            cond2='<'
                            cond_value2=list_two[0].split('<')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('>'))!=1:
                            cond_col2=list_two[0].split('>')[0]
                            cond2='>'
                            cond_value2=list_two[0].split('>')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        # print(cond_col2,cond2,cond_value2)
                        metadata,col_tab=read_metadata()
                        
                        tem2=cond_col2.split('.')
                        if len(tem2)==1:
                            count=0
                            for y in frm:
                                try:
                                    for z in col_tab[cond_col2]:
                                        if z==y:
                                            temp_col=y+'.'+cond_col2
                                            count+=1
                                        if count>1:
                                            print("ERROR !!! Ambiguity in column in where clause")
                                            exit(0)
                                except:
                                    print("ERROR !!! Incorrect column in where clause")
                                    exit(0)
                            cond_col2=temp_col
                            
                        if hell2==True:
                            tem2=cond_value2.split('.')
                            if len(tem2)==1:
                                count=0
                                for y in frm:
                                    try:
                                        for z in col_tab[cond_value2]:
                                            if z==y:
                                                temp_col=y+'.'+cond_value2
                                                count+=1
                                            if count>1:
                                                print("ERROR !!! Ambiguity in column in where clause")
                                                exit(0)
                                    except:
                                        print("ERROR !!! Incorrect column in where clause")
                                        exit(0)
                                cond_value2=temp_col

                        try:
                            indexx2=new_columns.index(cond_col2)
                            if hell2==True:
                                indexxx2=new_columns.index(cond_value2)
                        except:
                            print("ERROR !!! Incorrect column in where")
                            exit(0)

                        passing_data2=[]
                        if cond2=='<=':
                            for x in passing_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='>=':
                            for x in passing_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='=':
                            for x in passing_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='<':
                            for x in passing_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='>':
                            for x in passing_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        # print(passing_data2)
                        result=passing_data2
                        # print(result)
                    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
                    elif symbol=='or':

                        if len(list_one[0].split('<='))!=1:
                            cond_col=list_one[0].split('<=')[0]
                            cond='<='
                            cond_value=list_one[0].split('<=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('>='))!=1:
                            cond_col=list_one[0].split('>=')[0]
                            cond='>='
                            cond_value=list_one[0].split('>=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('='))!=1:
                            cond_col=list_one[0].split('=')[0]
                            cond='='
                            cond_value=list_one[0].split('=')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('<'))!=1:
                            cond_col=list_one[0].split('<')[0]
                            cond='<'
                            cond_value=list_one[0].split('<')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        elif len(list_one[0].split('>'))!=1:
                            cond_col=list_one[0].split('>')[0]
                            cond='>'
                            cond_value=list_one[0].split('>')[1]
                            if cond_value.isdigit()==False:
                                hell=True

                        # print(cond_col,cond,cond_value)
                        metadata,col_tab=read_metadata()
                        
                        tem0=cond_col.split('.')
                        if len(tem0)==1:
                            count=0
                            for y in frm:
                                try:
                                    for z in col_tab[cond_col]:
                                        if z==y:
                                            temp_col=y+'.'+cond_col
                                            count+=1
                                        if count>1:
                                            print("ERROR !!! Ambiguity in column in where clause")
                                            exit(0)
                                except:
                                    print("ERROR !!! Incorrect column in where clause")
                                    exit(0)
                            cond_col=temp_col

                        if hell1==True:
                            tem0=cond_value.split('.')
                            if len(tem0)==1:
                                count=0
                                for y in frm:
                                    try:
                                        for z in col_tab[cond_value]:
                                            if z==y:
                                                temp_col=y+'.'+cond_value
                                                count+=1
                                            if count>1:
                                                print("ERROR !!! Ambiguity in column in where clause")
                                                exit(0)
                                    except:
                                        print("ERROR !!! Incorrect column in where clause")
                                        exit(0)
                                cond_value=temp_col
                            
                        try:
                            indexx=new_columns.index(cond_col)
                            if cond_value.isdigit()==False:
                                indexxx=new_columns.index(cond_value)
                        except:
                            print("ERROR !!! Incorrect column in where")
                            exit(0)

                        passing_data=[]
                        if cond=='<=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])<=int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]<=x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='>=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])>=int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]>=x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='=':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])==int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]==x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='<':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])<int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]<x[indexxx]:
                                        passing_data.append(x)

                        elif cond=='>':
                            for x in new_data:
                                if cond_value.isdigit()==True:
                                    if int(x[indexx])>int(cond_value):
                                        passing_data.append(x)
                                else :
                                    if x[indexx]>x[indexxx]:
                                        passing_data.append(x)

                        # print(passing_data)

                        ##########################################

                        if len(list_two[0].split('<='))!=1:
                            cond_col2=list_two[0].split('<=')[0]
                            cond2='<='
                            cond_value2=list_two[0].split('<=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True

                        elif len(list_two[0].split('>='))!=1:
                            cond_col2=list_two[0].split('>=')[0]
                            cond2='>='
                            cond_value2=list_two[0].split('>=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('='))!=1:
                            cond_col2=list_two[0].split('=')[0]
                            cond2='='
                            cond_value2=list_two[0].split('=')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('<'))!=1:
                            cond_col2=list_two[0].split('<')[0]
                            cond2='<'
                            cond_value2=list_two[0].split('<')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        elif len(list_two[0].split('>'))!=1:
                            cond_col2=list_two[0].split('>')[0]
                            cond2='>'
                            cond_value2=list_two[0].split('>')[1]
                            if cond_value2.isdigit()==False:
                                hell2=True


                        # print(cond_col2,cond2,cond_value2)
                        metadata,col_tab=read_metadata()
                        
                        tem2=cond_col2.split('.')
                        if len(tem2)==1:
                            count=0
                            for y in frm:
                                try:
                                    for z in col_tab[cond_col2]:
                                        if z==y:
                                            temp_col=y+'.'+cond_col2
                                            count+=1
                                        if count>1:
                                            print("ERROR !!! Ambiguity in column in where clause")
                                            exit(0)
                                except:
                                    print("ERROR !!! Incorrect column in where clause")
                                    exit(0)
                            cond_col2=temp_col
                            
                        if hell2==True:
                            tem2=cond_value2.split('.')
                            if len(tem2)==1:
                                count=0
                                for y in frm:
                                    try:
                                        for z in col_tab[cond_value2]:
                                            if z==y:
                                                temp_col=y+'.'+cond_value2
                                                count+=1
                                            if count>1:
                                                print("ERROR !!! Ambiguity in column in where clause")
                                                exit(0)
                                    except:
                                        print("ERROR !!! Incorrect column in where clause")
                                        exit(0)
                                cond_value2=temp_col

                        try:
                            indexx2=new_columns.index(cond_col2)
                            if hell2==True:
                                indexxx2=new_columns.index(cond_value2)
                        except:
                            print("ERROR !!! Incorrect column in where")
                            exit(0)

                        passing_data2=[]
                        if cond2=='<=':
                            for x in new_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='>=':
                            for x in new_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='=':
                            for x in new_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='<':
                            for x in new_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        elif cond2=='>':
                            for x in new_data:
                                if cond_value2.isdigit()==True:
                                    if int(x[indexx2])<=int(cond_value2):
                                        passing_data2.append(x)
                                else :
                                    if x[indexx2]<=x[indexxx2]:
                                        passing_data2.append(x)

                        # print(passing_data2)
                        # print(passing_data)
                        # print()
                        # print(passing_data2)

                        result = copy.deepcopy(passing_data)

                        for p in passing_data2:
                            flag=False
                            for q in passing_data:
                                if p==q:
                                    flag=True
                                    break
                            if flag==False:
                                result.append(p)
                    # print(result)
                    else :
                        print("ERROR !!! incorrect symbol in where")
                        exit(0)
                    #.......................................................................

                    if len(frm)==1:
                        if len(select)==1:
                            istrue,func,col=check_aggregate(select)

                            if istrue==True:
                                select_aggregate(frm,func,col,result,new_columns)
                            elif select[0]=='*':
                                select_all(frm,is_distinct,result,new_columns)
                            else:
                                select_any(select,frm,is_distinct,result,new_columns,any_list)
                        else:
                            select_any(select,frm,is_distinct,result,new_columns,any_list)
                            
                    else :
                        # if select[0]=='*':
                        #     select_all(frm,is_distinct,result,new_columns)
                        # else:
                        #     select_any(select,frm,is_distinct,result,new_columns,any_list)
                        if len(select)==1:
                            istrue,func,col=check_aggregate(select)

                            if istrue==True:
                                select_aggregate(frm,func,col,result,new_columns)
                            elif select[0]=='*':
                                select_all(frm,is_distinct,result,new_columns)
                            else:
                                select_any(select,frm,is_distinct,result,new_columns,any_list)
                        else:
                            select_any(select,frm,is_distinct,result,new_columns,any_list)
            
            else :
                hell1=False
                if len(list_one[0].split('<='))!=1:
                    cond_col=list_one[0].split('<=')[0]
                    cond='<='
                    cond_value=list_one[0].split('<=')[1]
                    if cond_value.isdigit()==False:
                        hell=True

                elif len(list_one[0].split('>='))!=1:
                    cond_col=list_one[0].split('>=')[0]
                    cond='>='
                    cond_value=list_one[0].split('>=')[1]
                    if cond_value.isdigit()==False:
                        hell=True

                elif len(list_one[0].split('='))!=1:
                    cond_col=list_one[0].split('=')[0]
                    cond='='
                    cond_value=list_one[0].split('=')[1]
                    if cond_value.isdigit()==False:
                        hell=True

                elif len(list_one[0].split('<'))!=1:
                    cond_col=list_one[0].split('<')[0]
                    cond='<'
                    cond_value=list_one[0].split('<')[1]
                    if cond_value.isdigit()==False:
                        hell=True

                elif len(list_one[0].split('>'))!=1:
                    cond_col=list_one[0].split('>')[0]
                    cond='>'
                    cond_value=list_one[0].split('>')[1]
                    if cond_value.isdigit()==False:
                        hell=True

                # print(cond_col,cond,cond_value)
                metadata,col_tab=read_metadata()
                
                tem0=cond_col.split('.')
                if len(tem0)==1:
                    count=0
                    for y in frm:
                        try:
                            for z in col_tab[cond_col]:
                                if z==y:
                                    temp_col=y+'.'+cond_col
                                    count+=1
                                if count>1:
                                    print("ERROR !!! Ambiguity in column in where clause")
                                    exit(0)
                        except:
                            print("ERROR !!! Incorrect column in where clause")
                            exit(0)
                    cond_col=temp_col

                if hell1==True:
                    tem0=cond_value.split('.')
                    if len(tem0)==1:
                        count=0
                        for y in frm:
                            try:
                                for z in col_tab[cond_value]:
                                    if z==y:
                                        temp_col=y+'.'+cond_value
                                        count+=1
                                    if count>1:
                                        print("ERROR !!! Ambiguity in column in where clause")
                                        exit(0)
                            except:
                                print("ERROR !!! Incorrect column in where clause")
                                exit(0)
                        cond_value=temp_col
                    
                try:
                    indexx=new_columns.index(cond_col)
                    if cond_value.isdigit()==False:
                        indexxx=new_columns.index(cond_value)
                except:
                    print("ERROR !!! Incorrect column in where")
                    exit(0)

                result=[]
                if cond=='<=':
                    for x in new_data:
                        if cond_value.isdigit()==True:
                            if int(x[indexx])<=int(cond_value):
                                result.append(x)
                        else :
                            if x[indexx]<=x[indexxx]:
                                result.append(x)

                elif cond=='>=':
                    for x in new_data:
                        if cond_value.isdigit()==True:
                            if int(x[indexx])>=int(cond_value):
                                result.append(x)
                        else :
                            if x[indexx]>=x[indexxx]:
                                result.append(x)

                elif cond=='=':
                    for x in new_data:
                        if cond_value.isdigit()==True:
                            if int(x[indexx])==int(cond_value):
                                result.append(x)
                        else :
                            if x[indexx]==x[indexxx]:
                                result.append(x)

                elif cond=='<':
                    for x in new_data:
                        if cond_value.isdigit()==True:
                            if int(x[indexx])<int(cond_value):
                                result.append(x)
                        else :
                            if x[indexx]<x[indexxx]:
                                result.append(x)

                elif cond=='>':
                    for x in new_data:
                        if cond_value.isdigit()==True:
                            if int(x[indexx])>int(cond_value):
                                result.append(x)
                        else :
                            if x[indexx]>x[indexxx]:
                                result.append(x)

                if len(frm)==1:
                    if len(select)==1:
                        istrue,func,col=check_aggregate(select)

                        if istrue==True:
                            select_aggregate(frm,func,col,result,new_columns)
                        elif select[0]=='*':
                            select_all(frm,is_distinct,result,new_columns)
                        else:
                            select_any(select,frm,is_distinct,result,new_columns,any_list)
                    else:
                        select_any(select,frm,is_distinct,result,new_columns,any_list)
                        
                else :
                    # if select[0]=='*':
                    #     select_all(frm,is_distinct,result,new_columns)
                    # else:
                    #     select_any(select,frm,is_distinct,result,new_columns,any_list)
                    if len(select)==1:
                        istrue,func,col=check_aggregate(select)

                        if istrue==True:
                            select_aggregate(frm,func,col,result,new_columns)
                        elif select[0]=='*':
                            select_all(frm,is_distinct,result,new_columns)
                        else:
                            select_any(select,frm,is_distinct,result,new_columns,any_list)
                    else:
                        select_any(select,frm,is_distinct,result,new_columns,any_list)


# print(sys.argv[1])

if len(sys.argv)>2:
    print("Incorrect command line argument !!!")
    exit(0)
# print(sys.argv)
query=sys.argv[1]
if query[-1]!=';':
    print("Incorrect Query missing ';' " )
query=query[:-1]
main(query)