#-------------------------------------------------------------------------------
# Name:        DataProcess_ATK
# Purpose:
# Author:      xlin1x
# Email:       xiangx.lin@intel.com
# Created:     25/1/2016
#-------------------------------------------------------------------------------

# coding: utf-8

# In[10]:

# create connect to server
import trustedanalytics as ta
ta.server.uri='it_flex-e9c6c0af.demo-gotapaas.com'
ta.connect("/root/demo.creds")


# In[11]:

#create an array to maintain frame column
featureList = ["GXY", "Age", "Sex", "Height", "BMI", "DBP", "Cr", "HCT"];
#create an array to maintain classfiy opertion
classfiyList = ["DBP","HCT"];


# In[12]:

#create a schema 
csv = ta.CsvFile("hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/ef53c275-e12d-49ea-b2dd-bce12717b72b/000000_1",        
                  schema=[
                          ("GXY",ta.int32),
                          ("Age",ta.int32),
                          ("Sex",ta.int32),
                          ("Height",ta.float64),
                          ("BMI",ta.float64),
                          ("DBP",ta.float64),
                          ("Cr",ta.int32),
                          ("HCT",ta.float64)
                          ], skip_header_lines=1);


# In[27]:

#create frame
#frame_name = "myframe";
#if frame_name in ta.get_frame_names():
    #ta.drop_frames(frame_name)
    
    
frame_name = 'myframe'
exist_frames = ta.get_frame_names()
if frame_name in exist_frames:
    print "Frame exists, delete it"
    ta.drop_frames(frame_name)
        
my_frame = ta.Frame(csv, frame_name)
my_frame.inspect(21)


# In[31]:

#feature classify

def transformation_DBP(row):
    #＜60一组，60~90每10mmHg一组，≥90一组
    dbp = row.DBP
    if dbp < 60.0:
        dbp = 1;
    if dbp >= 60.0 and dbp < 70.0:
        dbp = 2;
    if dbp >= 70.0 and dbp < 80.0:
        dbp = 3;
    if dbp >= 80.0 and dbp < 90.0:
        dbp = 4;
    if dbp >= 90.0:
        dbp = 5;
    return dbp;
    

def transformation_HCT(row):
    hct = row.HCT    
    sex = row.Sex    
    if sex == 1:                                                                                                                      
        if hct < 40.0:            
            hct = 2;    
        if hct >= 40.0 and hct <= 50.0:
            hct = 0;
        if hct >= 50.0:
            hct = 1;
    if sex == 2:
        if hct < 37.0:
            hct = 2;
        if hct >= 37.0 and hct <= 48.0:
            hct = 0;
        if hct >= 48.0:
            hct = 1;
    return hct



#get a name for this feature when it's classfiy
def get_classfiyName(feature_name):
    feature_name = "classfiy_" + feature_name;
    return feature_name;

# a factory method for classify
def classify_factory(feature_name):
    if feature_name == "DBP":
        re_feature_name = get_classfiyName(feature_name);
        my_frame.add_columns(transformation_DBP, (re_feature_name, ta.int32));
        featureList.append(re_feature_name);
        my_frame.drop_columns(feature_name);
        my_frame.rename_columns({re_feature_name : feature_name})
        
    if feature_name == "HCT":
        re_feature_name = get_classfiyName(feature_name);
        my_frame.add_columns(transformation_HCT, (re_feature_name, ta.int32));
        featureList.append(re_feature_name);
        my_frame.drop_columns(feature_name);
        my_frame.rename_columns({re_feature_name : feature_name})

#execute classify
for c_name in classfiyList:
    classify_factory(c_name); 
my_frame.inspect(21)


# In[ ]:

# feature fill

#detection missing value
def detection_missing(feature_name, feature_index):
    ismissing = "0";
    count = my_frame.row_count
    rows_collection = my_frame.take(count) 
    for row in rows_collection:
        if row[feature_index] == "32768"
             ismissing = "1";
    return ismissing;


#avg
def calculate_avg(feature_name , feature_index):
    rows_collection = my_frame.take(count) 
    for row in rows_collection:
        if row[feature_index] != 32768.0
            row_array.append(row[feature_index]);
    
    sum_value = 0.0;
    avg_value = 0.0;
    for value in row_array:
        sum_value +=  value;
    avg_value = sum_value / len(row_array);
    return avg_value;



def calculate_median(feature_name ,feature_index):
    pass;
    
def calculate_mode(feature_name ,feature_index):
    pass;

def get_avg(feature_name ,feature_index):
    ismissing = "0";
    sum_value = 0.0;
    avg_value = 0.0;
    count = my_frame.row_count
    rows_collection = my_frame.take(count) 
    for row in rows_collection:
        sum_value +=  row;
    avg_value = sum_value / count;
    return avg_value;
        
def get_median(feature_name , feature_index):
    median_value = my_frame.column_median(data_column=feature_name);
    return median;

def get_mode(feature_name , feature_index):
    mode_result = my_frame.column_mode(data_column=feature_name);
    mode_value = mode_result["modes"];

    
def get_feature_index(featuerList , featuer_name):
    feature_index = -1;
    count= len(featuerList);
    for i in range(featuerList):
        if featuerList[i] == feature_name:
            feature_index = i;
            break;
    return feature_index;
    
#join columns        
def fill_opertion(fill_value,feature_name,feature_index):
    rows_collection = my_frame.take(count) 
    for row in rows_collection:
        if row[feature_index] != 32768.0
            row_array.append(row[feature_index]);          
    key_array = []
    for i in range(len(row_array)):
        temp=[]
        if row_array[i]= "32768"
            height[i] = average;
            temp.append(height[i]);
        else:
             temp.append(row_array)
    key_array.append(temp);
    
    #create a key frame
    fill_feature_name = "fill_" + feature_name ; 
    key_frame = ta.Frame(ta.UploadRows(key_array, [(feature_name, ta.float64),(fill_feature_name, ta.float64)]))
    
    #join the frame ,produce a new frame it can use for model
    model_frame = my_frame.join(key_frame,feature_name,how='left');   
    #drop temp frame
    
            
# a factory method for fill missing value    
def fill_factory(featureList , feature_name , fill_type):
    feature_index = get_feature_index(featuerList , featuer_name);
    if fill_type == "mode":
        fill_value = calculate_mode(feature_name,feature_index);
        fill_opertion(fill_value,feature_name,feature_index)
        
    if fill_type == "avg":
        fill_value = calculate_avg(feature_name,feature_index);
        fill_opertion(fill_value,feature_name,feature_index)
        
    if fill_type == "median:
        fill_value = calculate_median(feature_name,feature_index);
        fill_opertion(fill_value,feature_name,feature_index)


for i in range(featuerList):
     isMissing = detection_missing(featuerList[i], i);
     if isMissing == "1":
         fill_factory(featureList , featuerList[i] , "avg")
     
     

