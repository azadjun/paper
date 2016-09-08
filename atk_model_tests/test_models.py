#
# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import connect

#(Fengqian): Hard code here about the data path in HDFS

KMeans_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
              '71937f63-576c-41c0-bd75-97bb6dd0d5ce/000000_1'

LinearReg_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
                 'e217d079-644b-49ab-840b-a4e446af93ad/000000_1'

Naive_bayes_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
                   '2100dbae-0b1e-4813-b452-746f9417d9d4/000000_1'

Principal_com_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
                     'c70a84bd-3502-4549-894e-208290674452/000000_1'

Random_forest_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
                     'aa252bb6-cc23-4047-8cdd-fb6b016f9039/000000_1'

Svm_data = 'hdfs://nameservice1/org/intel/hdfsbroker/userspace/b61d4808-e761-45c3-bd54-afcb05b84a8b/' \
           '146f0fa8-27ff-4598-9bbf-1da860272eb9/000000_1'


def testKMeans(path, ta):

    print "define csv file"
    csv = ta.CsvFile(path, schema=[('data', ta.float64), ('name', str)], skip_header_lines=1)

    print "create frame"
    frame = ta.Frame(csv)

    print "Initializing a KMeansModel object"
    k = ta.KMeansModel(name='FeelKMeansModel')

    print "Training the model on the Frame"
    k.train(frame, ['data'], [2.0])
    
    output = k.predict(frame)
    print output.column_names



def testLinearRegression(path, ta):
    print "define csv file"
    csv = ta.CsvFile(path, schema=[("y", ta.float64), ("1", ta.float64), ("2", ta.float64), ("3", ta.float64),
                                   ("4", ta.float64), ("5", ta.float64), ("6", ta.float64), ("7", ta.float64),
                                   ("8", ta.float64), ("9", ta.float64), ("10", ta.float64)])

    print "create frame"
    frame = ta.Frame(csv, 'LinearRegressionSampleFrame')

    print "Initializing a LinearRegressionModel object"
    model = ta.LinearRegressionModel(name='myLinearRegressionModel')

    print "Training the model on the Frame"
    model.train(frame, 'y', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

    output = model.predict(frame)
    print output.column_names


def test_naive_bayes(path, ta):
    print "define csv file"
    schema = [("Class", ta.int32), ("Dim_1", ta.int32), ("Dim_2", ta.int32), ("Dim_3", ta.int32)]
    train_file = ta.CsvFile(path, schema=schema)
    print "creating the frame"
    train_frame = ta.Frame(train_file)

    print "initializing the naivebayes model"
    n = ta.NaiveBayesModel()

    print "training the model on the frame"
    n.train(train_frame, 'Class', ['Dim_1', 'Dim_2', 'Dim_3'])

    print "predicting the class using the model and the frame"
    output = n.predict(train_frame)
    print output.column_names


def test_principal_components(path, ta):
    print "define csv file"
    schema = [("1", ta.float64), ("2", ta.float64), ("3", ta.float64), ("4", ta.float64), ("5", ta.float64),
              ("6", ta.float64), ("7", ta.float64), ("8", ta.float64), ("9", ta.float64), ("10", ta.float64),
              ("11", ta.float64)]
    train_file = ta.CsvFile(path, schema=schema)
    print "creating the frame"
    train_frame = ta.Frame(train_file)

    print "initializing the principalcomponents model"
    p = ta.PrincipalComponentsModel()

    print "training the model on the frame"
    p.train(train_frame, ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"], k=9)

    print "predicting the class using the model and the frame"
    output = p.predict(train_frame, c=5, t_squared_index=True)

    print output.column_names


def test_RandomForest(path, ta):
    print "define csv file"
    csv = ta.CsvFile(path, schema=[('Class', int), ('Dim_1', ta.float64), ('Dim_2', ta.float64)])

    print "create frame"
    frame = ta.Frame(csv)

    print "Initializing the classifier model object"
    classifier = ta.RandomForestClassifierModel()

    print "Training the model on the Frame"
    classifier.train(frame, 'Class', ['Dim_1', 'Dim_2'], num_classes=2)

    print "Predicting on the Frame"
    output = classifier.predict(frame)
    print output.column_names

    #(output.column_names, ['Class', 'Dim_1','Dim_2', 'predicted_class'])

    print "Initializing the classifier model object"
    regressor = ta.RandomForestRegressorModel()

    print "Training the model on the Frame"
    regressor.train(frame, 'Class', ['Dim_1', 'Dim_2'])

    print "Predicting on the Frame"
    regressor_output = regressor.predict(frame)
    print regressor_output.column_names

    #(regressor_output.column_names, ['Class', 'Dim_1','Dim_2', 'predicted_value'])

def test_Svm(path, ta):
    print "define csv file"
    csv = ta.CsvFile(path, schema=[('data', ta.float64), ('label', str)], skip_header_lines=1)

    print "create frame"
    frame = ta.Frame(csv)

    print "Initializing a SvmModel object"
    k = ta.SvmModel(name='mySvmModel')

    print "Training the model on the Frame"
    k.train(frame, 'label', ['data'])

    print "Predicting on the Frame"
    m = k.predict(frame)
    print m.column_names

    #m.column_names, ['data', 'label', 'predicted_label'])


if __name__ == '__main__':
    ta = connect.connect()
    if ta is None:
        raise Exception

#    testKMeans(KMeans_data, ta)
#    testLinearRegression(LinearReg_data, ta)
#    test_naive_bayes(Naive_bayes_data, ta)
#    test_principal_components(Principal_com_data, ta)
#    test_RandomForest(Random_forest_data, ta)
    test_Svm(Svm_data, ta)


