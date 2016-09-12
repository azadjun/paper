# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.server.uri = "atk-c4510a4d-f1ce-44f8-b4a6.10.239.165.216.xip.io"
ta.loggers.set_http()
#ta.loggers.set_api()

ta.connect("/home/yilan/atk/atk_poc/demo.creds")

class ModelNaiveBayesTest(unittest.TestCase):

    def test_naive_bayes(self):
        print "define csv file"

        csv = ta.CsvFile("hdfs://nameservice1/org/intel/hdfsbroker/userspace/ae6a38d3-191f-494f-86a6-3fe1b2255902/e3327582-f475-4dc9-8efa-96070abb606d/000000_1",
                         schema=[
                          ("GXY",ta.int32),
                          #("HPI",ta.ignore),
                          ("Age",ta.int32),
                          ("Sex",ta.int32),
                          ("Height",ta.float64),
                          ("Weight",ta.float64),
                          ("BMI",ta.float64),
                          ("DBP",ta.float64),
                          ("SBP",ta.float64),
                          ("HCT",ta.float64),
                          ("MCV",ta.float64),
                          ("RDW_SD",ta.float64),
                          ("RDW_CV",ta.float64),
                          ("HGB",ta.float64),
                          ("MCH",ta.float64),
                          ("MCHC",ta.float64),
                          ("RBC",ta.float64),
                          ("WBC",ta.float64),
                          ("NEUT1",ta.float64),
                          ("LYMPH",ta.float64),
                          ("MONO1",ta.float64),
                          ("EO1",ta.float64),
                          ("BASO1",ta.float64),
                          ("NEUT2",ta.float64),
                          ("MONO2",ta.float64),
                          ("EO2",ta.float64),
                          ("BASO2",ta.float64),
                          ("PLT",ta.float64),
                          #("PDW",ta.ignore),
                          ("MPV",ta.float64),
                          ("P_LCR",ta.float64),
                          ("PCT",ta.float64),
                          ("Lymph_3",ta.float64),
                          ("ESR",ta.float64),
                          ("PH",ta.float64),
                          ("PRO",ta.float64),
                          ("GIu",ta.float64),
                          ("KET",ta.float64),
                          ("BLD",ta.float64),
                          ("BIL",ta.float64),
                          ("URO",ta.float64),
                          ("NIT",ta.float64),
                          ("SG",ta.float64),
                          ("LEU",ta.float64),
                          ("N_QT",ta.float64),
                          ("VC",ta.float64),
                          #("ECG",ta.ignore),
                          #("BCJC1",ta.ignore),
                          #("IRDS",ta.ignore),
                          #("WK",ta.ignore),
                          ("OB",ta.float64),
                          ("FBG",ta.float64),
                          ("HBsAg",ta.float64),
                          ("HBsAb",ta.float64),
                          ("HBeAg",ta.float64),
                          ("HBeAb",ta.float64),
                          ("HBcAb",ta.float64),
                          ("TBiL",ta.float64),
                          ("ALT",ta.float64),
                          ("AST",ta.float64),
                          ("AKP",ta.float64),
                          ("GGT",ta.float64),
                          ("ADA",ta.float64),
                          ("TPO",ta.float64),
                          ("Aib",ta.float64),
                          ("Gib",ta.float64),
                          ("A_G",ta.float64),
                          ("PA",ta.float64),
                          ("AST_ALT",ta.float64),
                          ("BUN",ta.float64),
                          ("Cr",ta.float64),
                          ("UA",ta.float64),
                          ("CK",ta.float64),
                          ("LDH",ta.float64),
                          ("CK_MB",ta.float64),
                          ("LDH_MB",ta.float64),
                          ("a_HBD",ta.float64),
                          ("TNI",ta.float64),
                          ("Fg",ta.float64),
                          ("K1",ta.float64),
                          ("AFP",ta.float64),
                          ("CEA",ta.float64),
                          ("Free_PSA",ta.float64),
                          ("CA125",ta.float64),
                          ("CA19_9",ta.float64),
                          ("NSE",ta.float64),
                          ("CA242",ta.float64),
                          ("B_HCG",ta.float64),
                          ("CA15_3",ta.float64),
                          ("CA50",ta.float64),
                          ("CA72_4",ta.float64),
                          ("HGH",ta.float64),
                          ("SF",ta.float64),
                          ("QJD",ta.float64),
                          ("DCJC",ta.float64),
                          ("MJJC",ta.float64),
                          ("RUT",ta.float64),
                          ("PGI_PGII",ta.float64),
                          ("Ca2",ta.float64),
                          ("P3",ta.float64),
                          ("K2",ta.float64),
                          ("Na",ta.float64),
                          ("CI",ta.float64)
                          ], skip_header_lines=1)

        print "create frame"
        frame_name = 'ModelNaiveBayesFrame'
        exist_frames = ta.get_frame_names()
        if frame_name in exist_frames:
            print "Frame exists, delete it"
            ta.drop_frames(frame_name)
        train_frame = ta.Frame(csv, frame_name)

        print "Initializing a RandomForestModel object"
        model_name = 'POCModelNaiveBayesModel'
        exist_models = ta.get_model_names()
        if model_name in exist_models:
            print "Model exist, delete"
            ta.drop_models(model_name)
        naive = ta.NaiveBayesModel(name=model_name)

        print "Training the model on the Frame"
        naive.train(train_frame,'GXY',['Age','Sex','Height','Weight','BMI','DBP','SBP','HCT','MCV','RDW_SD',
                                 'RDW_CV','HGB','MCH','MCHC','RBC','WBC','NEUT1','LYMPH','MONO1','EO1','BASO1','NEUT2',
                                 'MONO2','EO2','BASO2','PLT','MPV','P_LCR','PCT','Lymph_3','ESR','PH','PRO',
                                 'GIu','KET','BLD','BIL','URO','NIT','SG','LEU','N_QT','VC',
                                 'OB','FBG','HBsAg','HBsAb','HBeAg','HBeAb','HBcAb','TBiL','ALT','AST','AKP','GGT',
                                 'ADA','TPO','Aib','Gib','A_G','PA','AST_ALT','BUN','Cr','UA','CK','LDH','CK_MB',
                                 'LDH_MB','a_HBD','TNI','Fg','K1','AFP','CEA','Free_PSA','CA125','CA19_9','NSE','CA242',
                                 'B_HCG','CA15_3','CA50','CA72_4','HGH','SF','QJD','DCJC','MJJC','RUT','PGI_PGII',
                                 'Ca2','P3','K2','Na','CI'],num_classes=2)


        print "Predicting on the Frame"
        output = naive.predict(train_frame)

        self.assertEqual(output.column_names,['GXY','Age','Sex','Height','Weight','BMI','DBP','SBP','HCT',
                                              'MCV','RDW_SD','RDW_CV','HGB','MCH','MCHC','RBC','WBC','NEUT1','LYMPH',
                                              'MONO1','EO1','BASO1','NEUT2','MONO2','EO2','BASO2','PLT','MPV',
                                              'P_LCR','PCT','Lymph_3','ESR','PH','PRO','GIu','KET','BLD','BIL','URO',
                                              'NIT','SG','LEU','N_QT','VC','OB','FBG','HBsAg',
                                              'HBsAb','HBeAg','HBeAb','HBcAb','TBiL','ALT','AST','AKP','GGT','ADA',
                                              'TPO','Aib','Gib','A_G','PA','AST_ALT','BUN','Cr','UA','CK','LDH',
                                              'CK_MB','LDH_MB','a_HBD','TNI','Fg','K1','AFP','CEA','Free_PSA','CA125',
                                              'CA19_9','NSE','CA242','B_HCG','CA15_3','CA50','CA72_4','HGH','SF','QJD',
                                              'DCJC','MJJC','RUT','PGI_PGII','Ca2','P3','K2','Na','CI','predicted_class'])

if __name__ == "__main__":
    unittest.main()