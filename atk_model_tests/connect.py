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


def connect():
    import trustedanalytics as ta
    ta.server.uri = "atk-34157d69-65f4-426f-ac14.demo-gotapaas.com"
    ta.loggers.set_api()
    ta.connect('/root/demo.creds')

    return ta if ta is not None else None
