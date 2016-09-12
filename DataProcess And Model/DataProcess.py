#-------------------------------------------------------------------------------
# Name:        DataProcess
# Purpose:
# Author:      xlin1x
# Email:       xiangx.lin@intel.com
# Created:     25/11/2015
#-------------------------------------------------------------------------------

#packages import
import shutil
import os
import codecs
import sys

#------------global variable----------------------------
Read_Split_Flag = '\t'
Write_Split_Flag=','

Sex_Default_Man='1'
Sex_Default_Women='2'
GXY_Yes='1'
GXY_No='0'
Default_Value='32768'

#------------File read and write operation--------------

#Read the content for list from file
def ReadFromFile(file_path):

     lines_List=[]
     try:
         fs = codecs.open(file_path, encoding='utf-8')
         lines_List =fs.readlines()
         fs.close()
     except IOError:
       print("fail to open file")
     return lines_List;

#Write the list to file
def WriteToFile(lines_List,file_path):
     try:
        fs = codecs.open(file_path,'w','utf-8')
        for item in lines_List:
            fs.write(str(item))
        fs.close()
     except IOError:
        print("fail to open file")


#------------Function1: Fill the default value------------

def GetIndexsFromColName(col_Name , line_data):
    cols_items= line_data[0].split(Read_Split_Flag);




def FillDefaultValue(line_data):
    #declar global
    global Read_Split_Flag
    global Write_Split_Flag

    global Sex_Default_Man
    global Sex_Default_Women
    global GXY_Yes
    global GXY_No
    global Default_Value

    #get cols items
    cols_items= line_data[0].split(Read_Split_Flag);
    print line_data[1].split(Read_Split_Flag);
    #Loop rows
    for i in range(len(line_data)):
        line = line_data[i];
        groupArray = line.split(Read_Split_Flag);#split row

        #Loop cols
        for j in range(len(cols_items)):
            value=groupArray[j];
            col_Name=cols_items[j];

            if col_Name =="FinalCheck":
                #replace anomaly char in string
                value = value.replace(Read_Split_Flag,';')
                value = value.replace(',','%')
                groupArray[j] = value

            if col_Name =="Sex" and value == '男':
                groupArray[j] = Sex_Default_Man

            if col_Name =="Sex" and value == '女':
                groupArray[j] = Sex_Default_Women

            if col_Name =="GXY" and value == 'Yes':
                groupArray[j] = GXY_Yes

            if col_Name =="GXY" and value == 'No':
                groupArray[j] = GXY_No

            if value == '' or value == '\n' or value == '\r\n':
                groupArray[j] = Default_Value

            #groupArray[j] = groupArray[j].replace(';','')
            #groupArray[j] = RemoveLastStr(groupArray[j])

        #create new rows and add to the lines_data
        newline=''
        for j in range(len(cols_items)):
            if j < len(cols_items)-1:
                newline += groupArray[j] + Write_Split_Flag
            else:
                newline += groupArray[j]

        if i > 0:
           newline += '\n'

        #print newline;
        line_data[i]=newline;
    return line_data;


def RemoveLastStr(value):
    valueStr = str(value);
    value =valueStr[-1].replace(';','')
    return valueStr;


#------------Function2: Extract the data column-----------

def ExtractDataFromCols(col_array,line_data):
    global Read_Split_Flag
    global Write_Split_Flag

    list=[];

    colgroup=line_data[0].split(Write_Split_Flag)
    #print Read_Split_Flag
    #print colgroup
    for i in range(len(line_data)):
        line = line_data[i];

        newline=""
        groupArray = line.split(Write_Split_Flag);
        #print groupArray
        for j in range(len(groupArray)):
            colName=colgroup[j]
            #print colName
            for c in range(len(col_array)):
                if colName.strip() == col_array[c].strip():
                    if c < len(col_array) -1:
                        newline += groupArray[j] + Write_Split_Flag
##                        if groupArray[j].count(".") >  1  :
##                           if IsInList(colName,list) == 1:
##                                 list.append(colName.strip())
##                                 print colName.strip()

                    else:
                        newline += groupArray[j]
        #print newline;
        line_data[i]=newline+'\n';
    return line_data

def IsInList(col_Name , list):
     count =1;
     for x in range(len(list)):
         if x == col_Name.strip():
            count=0
     return count;


#------------Execute  Script---------------------------------------
def main():
    #char endoing
    reload(sys)
    sys.setdefaultencoding("utf-8")

    #file & config path
    sourceFilePath='SampleData.txt'
    writeFillFilePath ='Result_Fill_Data.csv'

    extractConfig="extractConfig.txt"
    extractFilePath='Result_Extract_Data.csv'

    #fill  data
    line_data = ReadFromFile(sourceFilePath);
    fill_data = FillDefaultValue(line_data);
    #WriteToFile(fill_data,writeFillFilePath);

    #Extract data
    Extract_config = ReadFromFile(extractConfig)
    Extract_data=ExtractDataFromCols(Extract_config,fill_data)
    #WriteToFile(Extract_data,extractFilePath);


if __name__ == '__main__':
    main()
