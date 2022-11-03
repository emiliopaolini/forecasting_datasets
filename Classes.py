# Copyright 2021 Scuola Superiore Sant'Anna www.santannapisa.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python imports

from confluent_kafka import KafkaError, KafkaException
import json
from confluent_kafka.error import ConsumeError

from io import StringIO
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from algorithms.lstmCpu import lstmcpu
from algorithms.lstmRAMDT import lstmramdt

import logging
import copy

log = logging.getLogger("Forecaster")


class ForecastingJob:
    """
    Make it a thread with a consumer with consumerid=id.
    Each message mast be added with the add data function.
    https://github.com/confluentinc/confluent-kafka-python
    """

    def __init__(self, idx, data_type, model, metric, il, instance_name, steps=None, output=None, outTopic=None):
        self.model = model
        self.job_id = idx
        self.nstype = data_type
        self.forecast = False
        self.names = {}
        self.metric = metric
        if steps is None:
            self.time_steps = 10
        else:
            self.time_steps = steps
        self.batch_size = 10
        self.instance_name = instance_name
        if self.model == "Test":
            self.data = np.arange(self.time_steps).reshape(self.time_steps, 1)
        else:
            self.data = {}
        self.trained_model = None
        self.lstm_data = None
        self.il = il
        self.datalist = []
        self.temp = {}
        self.set_temp = {}
        self.r1 = []
        self.r2 = []
        self.other_robs = []
        self.back = 10
        self.forward = 4
        self.features = ['avg_rtt_a1', 'avg_rtt_a2', 'avg_loss_a1', 'avg_loss_a2', 'r_a1', 'r_a2']
        self.main_feature = 'cpu0'
        self.update_robots = True
        self.csv = "data/data_.csv"
        self.save = False
        self.t0 = {}
        self.set_t0 = {}
        self.producer = output
        self.outTopic = outTopic
        self.loaded_json = {}

        if os.path.isfile(self.csv):
            [root, end] = self.csv.split('.')
            for i in range(0, 100):
                self.csv = root + self.instance_name + "_" + str(i) + "." + end
                if not os.path.isfile(self.csv):
                    break
        log.info(self.instance_name + " save data on file {}".format(self.csv))

    def data_parser1(self, json_data, avg):
        loaded_json = json.loads(json_data)
        log.debug(self.instance_name + ": forecasting Job: received data: \n{}".format(loaded_json))
        names = {}
        if self.model == "Test":
            if "cpu" or "CPU" or "Cpu" in self.metric:
                for element in loaded_json:
                    mtype = element['type_message']
                    if mtype == "metric":
                        instance = element['metric']['instance']
                        cpu = element['metric']['cpu']
                        mode = element['metric']['mode']
                        # nsid = element['metric']['nsId']
                        # vnfdif = element['metric']['vnfdId']
                        # t = element['value'][0]
                        val = element['value'][1]
                        if instance not in names.keys():
                            names[instance] = {}
                            names[instance]['cpus'] = []
                            names[instance]['modes'] = []
                        names[instance]['cpus'].append(cpu)
                        names[instance]['modes'].append(mode)
                        a1 = np.array([[round(float(val), 2)]])
                        self.add_data(a1)
                self.names = names
        if self.model == "lstmCPUBase":
            if "cpu" or "CPU" or "Cpu" in self.metric:
                for element in loaded_json:
                    mtype = element['type_message']
                    if mtype == "metric":
                        instance = element['metric']['instance']
                        if instance == self.instance_name:
                            cpu = element['metric']['cpu']
                            mode = element['metric']['mode']
                            # nsid = element['metric']['nsId']
                            # vnfdif = element['metric']['vnfdId']
                            t = element['value'][0]
                            val = element['value'][1]
                            if instance not in names.keys():
                                names[instance] = {}
                                names[instance]['cpus'] = []
                                names[instance]['modes'] = []
                                names[instance]['values'] = []
                                names[instance]['timestamp'] = []
                            names[instance]['cpus'].append(cpu)
                            names[instance]['modes'].append(mode)
                            names[instance]['values'].append(round(float(val), 2))
                            names[instance]['timestamp'].append(t)
                    self.names = names
                    avg_cpu = 0
                    t = None
                    # TODO: avoid the average
                    if avg:
                        for key in names.keys():
                            avg_cpu = sum(names[key]['values']) / len(names[key]['values'])
                            if t is None:
                                t = names[key]['timestamp'][0]
                        string = str(t) + ";" + str(self.il) + ";" + str(avg_cpu) + ";48;1"
                        self.lstm_data = StringIO(string + "\n")
                        # self.lstm_data = StringIO("col1;col2;col3;col4;col5\n" + string + "\n")

                        self.datalist.append(StringIO(string + "\n"))
                        if len(self.datalist) > 100:
                            del self.datalist[0]
                    else:
                        string = ""
                        for key in names.keys():
                            t = names[key]['timestamp'][0]
                            string = str(t)
                            for c in range(0, len(names[key]['cpus'])):
                                string = string + ";" + str(names[key]['values'][c])
                        self.datalist.append(StringIO(string + "\n"))
        else:
            log.error("Forecasting Job: model not supported")

    def data_parser2(self, json_data):
        loaded_json = json.loads(json_data)
        log.debug(self.instance_name + ": forecasting Job: received data: \n{}".format(loaded_json))
        names = {}
        if self.model == "lstmRAMEnhanced":
            loaded_json = json.loads(json_data)
            for element in loaded_json:
                mtype = element['type_message']
                if mtype == "metric":
                    m = element['metric']['__name__']
                    if "cpu" in m:
                        self.set_temp['node_cpu_seconds_total'] = True
                        cpu = element['metric']['cpu']
                        instance = element['metric']['instance']
                        if instance == self.instance_name:
                            t = element['value'][0]
                            val = round(float(element['value'][1]), 2)
                            if 'cpu' not in self.temp.keys():
                                self.temp['cpu'] = []
                            if cpu not in self.temp['cpu']:
                                self.temp['cpu'].append(cpu)
                            if 'time' not in self.temp.keys():
                                self.temp['time'] = t
                            if 'cpuv' not in self.temp.keys():
                                self.temp['cpuv'] = []
                            index = self.temp['cpu'].index(cpu)
                            if len(self.temp['cpuv']) == index:
                                self.temp['cpuv'].append(val)
                            else:
                                self.temp['cpuv'][index] = val
                                ######
                            mode = element['metric']['mode']
                            if instance not in names.keys():
                                names[instance] = {}
                                names[instance]['cpus'] = []
                                names[instance]['modes'] = []
                                names[instance]['values'] = []
                                names[instance]['timestamp'] = []
                                names[instance]['cpus'].append(cpu)
                                names[instance]['modes'].append(mode)
                                names[instance]['values'].append(round(float(val), 2))
                                names[instance]['timestamp'].append(t)
                            self.names = names
                            log.debug(self.instance_name + ": naming data acquired: \n{}".format(names))
                    elif "rtt" in m:
                        rob = element['metric']['robot_id']
                        t = element['value'][0]
                        if 'rtt_id' not in self.temp.keys():
                            self.temp['rtt_id'] = []
                        if rob in self.temp['rtt_id'] or "." in rob:
                            continue
                        self.temp['rtt_id'].append(rob)
                        if 'time' not in self.temp.keys():
                            self.temp['time'] = t
                        if 'rttv' not in self.temp.keys():
                            self.temp['rttv'] = []
                        val = round(float(element['value'][1]), 2)
                        self.temp['rttv'].append(val)
                        log.debug(self.instance_name + ": robot {} rtt_latency {} received".format(rob, str(val)))
                        log.debug(self.instance_name + ": temp db is:\n{} ".format(self.temp))
                        self.set_temp['rtt_latency'] = True
                        '''
                    elif "upstream" in m:
                        self.set_temp['upstream_latency'] = True
                        rob = element['metric']['robot_id']
                        t = element['value'][0]
                        val = round(float(element['value'][1]), 2)
                        if not 'up_id' in self.temp.keys():
                            self.temp['up_id'] = []
                        self.temp['up_id'].append(rob)
                        if not 'time' in self.temp.keys():
                            self.temp['time'] = t
                        if not 'upv' in self.temp.keys():
                            self.temp['upv'] = []
                        self.temp['upv'].append(val)
                        '''
                    elif "cmd_send" in m:
                        rob = element['metric']['robot_id']
                        t = element['value'][0]
                        val = round(float(element['value'][1]), 5)
                        if 'coms_id' not in self.temp.keys():
                            self.temp['coms_id'] = []
                        if rob in self.temp['coms_id'] or "." in rob:
                            continue
                        self.temp['coms_id'].append(rob)
                        if 'time' not in self.temp.keys():
                            self.temp['time'] = t
                        if 'comsv' not in self.temp.keys():
                            self.temp['comsv'] = []
                        self.temp['comsv'].append(val)
                        log.debug(self.instance_name + ": robot {} cmd_sent {} received".format(rob, str(val)))
                        log.debug(self.instance_name + ": temp db is:\n{} ".format(self.temp))
                        self.set_temp['cmd_sent'] = True
                    elif "cmd_lost" in m:
                        rob = element['metric']['robot_id']
                        t = element['value'][0]
                        val = round(float(element['value'][1]), 5)
                        if 'coml_id' not in self.temp.keys():
                            self.temp['coml_id'] = []
                        if rob in self.temp['coml_id'] or "." in rob:
                            continue
                        self.temp['coml_id'].append(rob)
                        if 'time' not in self.temp.keys():
                            self.temp['time'] = t
                        if 'comlv' not in self.temp.keys():
                            self.temp['comlv'] = []
                        self.temp['comlv'].append(val)
                        log.debug(self.instance_name + ": robot {} cmd_lost {} received".format(rob, str(val)))
                        log.debug(self.instance_name + ": temp db is:\n{} ".format(self.temp))
                        self.set_temp['cmd_lost'] = True
            #self.inject_data()
            self.inject_data2()
        else:
            log.error("Forecasting Job: model not supported")

    def savedata(self):
        csv_file = open(self.csv, "a")
        string = ""
        for key in self.data.keys():
            string = string + str(self.data[key][-1]) + ";"
        string = string[:-1] + "\n"
        csv_file.write(string)
        csv_file.close()

    def inject_data(self):
        if self.set_temp['node_cpu_seconds_total'] and self.set_temp['rtt_latency'] and \
                self.set_temp['cmd_sent'] and self.set_temp['cmd_lost']:
            temp1 = self.temp.copy()
            if 'cpu' in temp1.keys():
                for i in range(0, len(temp1['cpuv'])):
                    # string = string + ";" + str(temp1['cpuv'][i])
                    label = "cpu" + str(temp1['cpu'][i])
                    if label in self.data.keys():
                        if len(self.data[label]) == self.batch_size:
                            del self.data[label][0]
                            log.debug(self.instance_name+ "forecasting Job, Deleting older element: \n{}".format(self.data[label]))
                    else:
                        self.data[label] = []
                    self.data[label].append(round(float(temp1['cpuv'][i]), 1))
                    log.debug(self.instance_name+ "forecasting Job, current data for {}, after the addition: \n{}".format(label, self.data[label]))
            if 'rttv' in temp1.keys():
                val_a1 = 0.0
                val_a2 = 0.0
                for i in range(0, len(temp1['rttv'])):
                    rtt = round(float(temp1['rttv'][i]), 2)
                    # action type 1
                    if rtt < 150:
                        if temp1['rtt_id'][i] not in self.other_robs:
                            if temp1['rtt_id'][i] not in self.r2:
                                if temp1['rtt_id'][i] in self.r1:
                                    val_a1 = val_a1 + rtt
                                else:
                                    if self.update_robots:
                                        val_a1 = val_a1 + rtt
                                        self.r1.append(temp1['rtt_id'][i])
                                        log.info(self.instance_name + " added robot type1 {}".format(temp1['rtt_id'][i]))
                            else:
                                if self.update_robots:
                                    if temp1['rtt_id'][i] not in self.r1:
                                        val_a1 = val_a1 + rtt
                                        self.r1.append(temp1['rtt_id'][i])
                                        for index in range(0, len(self.r2)):
                                            if self.r2[index] == temp1['rtt_id'][i]:
                                                self.r2.pop(index)
                    # action type 2
                    else:
                        if temp1['rtt_id'][i] not in self.other_robs:
                            if temp1['rtt_id'][i] not in self.r1:
                                if temp1['rtt_id'][i] in self.r2:
                                    val_a2 = val_a2 + rtt
                                else:
                                    if self.update_robots:
                                        val_a2 = val_a2 + rtt
                                        self.r2.append(temp1['rtt_id'][i])
                                        log.info(self.instance_name + " added robot type2 {}".format(temp1['rtt_id'][i]))
                            else:
                                if self.update_robots:
                                    if temp1['rtt_id'][i] not in self.r2:
                                        val_a2 = val_a2 + rtt
                                        self.r2.append(temp1['rtt_id'][i])
                                        for index in range(0, len(self.r1)):
                                            if self.r1[index] == temp1['rtt_id'][i]:
                                                self.r1.pop(index)
                label = "avg_rtt_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r1) != 0:
                    val = round((val_a1 / len(self.r1)), 1)
                else:
                    val = 0.0
                self.data[label].append(val)

                label = "avg_rtt_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r2) != 0:
                    val = round((val_a2 / len(self.r2)), 1)
                else:
                    val = 0.0
                self.data[label].append(val)
            '''
            if 'upv' in temp1.keys():
                avg_up = sum(temp1['upv']) / len(temp1['upv'])
                string = string + ";" + str(round(avg_up, 2))
                
            '''
            if 'comlv' in temp1.keys() and 'comsv' in temp1.keys():
                lv_a1 = 0.0
                lv_a2 = 0.0
                sv_a1 = 0.0
                sv_a2 = 0.0

                # avg_coml = sum(temp1['comlv']) / len(temp1['comlv'])
                # avg_coms = sum(temp1['comsv']) / len(temp1['comsv'])
                for i in range(0, len(temp1['comlv'])):
                    lv = int(temp1['comlv'][i])
                    sv = int(temp1['comsv'][i])
                    if temp1['coml_id'][i] in self.r1:
                        lv_a1 = lv_a1 + lv
                        sv_a1 = sv_a1 + sv
                    elif temp1['coml_id'][i] in self.r2:
                        lv_a2 = lv_a2 + lv
                        sv_a2 = sv_a2 + sv
                    elif temp1['coml_id'][i] in self.other_robs:
                        log.info("Robot handled {} by other jobs ".format(temp1['coml_id'][i]))
                if sv_a1 != 0:
                    l_a1 = round(float(lv_a1 / sv_a1), 5)
                else:
                    l_a1 = 0.0
                if sv_a2 != 0:
                    l_a2 = round(float(lv_a2 / sv_a2), 5)
                else:
                    l_a2 = 0.0
                label = "avg_loss_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r1) != 0:
                    val = round((l_a1 / len(self.r1)), 5)
                else:
                    val = "0.0"
                self.data[label].append(val)
                label = "avg_loss_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r2) != 0:
                    val = round((l_a2 / len(self.r2)), 5)
                else:
                    val = "0.0"
                self.data[label].append(val)
            robs = self.r1 + self.r2
            if robs != 0:
                #label = "#robots"
                #if label in self.data.keys():
                #    if len(self.data[label]) == self.batch_size:
                #        del self.data[label][0]
                #else:
                #    self.data[label] = []
                #self.data[label].append(robs)
                label = "r_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(len(self.r1))
                log.info("{}->{}".format(label, self.data[label]))
                label = "r_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(len(self.r2))
                log.info("{}->{}".format(label, self.data[label]))

    def inject_data2(self):
        #if self.set_temp['node_cpu_seconds_total'] and self.set_temp['rtt_latency'] and \
        #        self.set_temp['cmd_sent'] and self.set_temp['cmd_lost']:
        if self.set_temp['node_cpu_seconds_total']:
            log.debug("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXAll data received\n")
            temp1 = self.temp.copy()
            log.debug("temp1 copy \n{}".format(temp1))
            if 'cpu' in temp1.keys():
                for i in range(0, len(temp1['cpuv'])):
                    # string = string + ";" + str(temp1['cpuv'][i])
                    label = "cpu" + str(temp1['cpu'][i])
                    if label in self.data.keys():
                        if len(self.data[label]) == self.batch_size:
                            del self.data[label][0]
                            log.debug(self.instance_name + " forecasting Job, Deleting older element: \n{}".format(
                                self.data[label]))
                    else:
                        self.data[label] = []
                    if not label in self.set_t0.keys() or not self.set_t0[label]:
                        self.t0[label] = temp1['cpuv'][i]
                        self.set_t0[label] = True
                    self.data[label].append(round(temp1['cpuv'][i]-self.t0[label], 2))
                    log.debug(
                        self.instance_name + " forecasting Job, current data for {}, after the addition: \n{}".format(
                            label, self.data[label]))
            if 'rttv' in temp1.keys():
                val_a1 = 0.0
                val_a2 = 0.0
                for i in range(0, len(temp1['rttv'])):
                    rtt = round(float(temp1['rttv'][i]), 2)
                    # action type 1
                    if rtt < 150:
                        if temp1['rtt_id'][i] not in self.other_robs:
                            if temp1['rtt_id'][i] not in self.r2:
                                if temp1['rtt_id'][i] in self.r1:
                                    val_a1 = val_a1 + rtt
                                else:
                                    if self.update_robots:
                                        val_a1 = val_a1 + rtt
                                        self.r1.append(temp1['rtt_id'][i])
                                        log.info(self.instance_name + " added robot type1 {}".format(temp1['rtt_id'][i]))
                            else:
                                if self.update_robots:
                                    if temp1['rtt_id'][i] not in self.r1:
                                        val_a1 = val_a1 + rtt
                                        self.r1.append(temp1['rtt_id'][i])
                                        for index in range(0, len(self.r2)):
                                            if self.r2[index] == temp1['rtt_id'][i]:
                                                self.r2.pop(index)
                    # action type 2
                    else:
                        if temp1['rtt_id'][i] not in self.other_robs:
                            if temp1['rtt_id'][i] not in self.r1:
                                if temp1['rtt_id'][i] in self.r2:
                                    val_a2 = val_a2 + rtt
                                else:
                                    if self.update_robots:
                                        val_a2 = val_a2 + rtt
                                        self.r2.append(temp1['rtt_id'][i])
                                        log.info(self.instance_name + " added robot type2 {}".format(temp1['rtt_id'][i]))
                            else:
                                if self.update_robots:
                                    if temp1['rtt_id'][i] not in self.r2:
                                        val_a2 = val_a2 + rtt
                                        self.r2.append(temp1['rtt_id'][i])
                                        for index in range(0, len(self.r1)):
                                            if self.r1[index] == temp1['rtt_id'][i]:
                                                self.r1.pop(index)
                label = "avg_rtt_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r1) != 0:
                    val = round((val_a1 / len(self.r1)), 2)
                else:
                    val = 0.0
                self.data[label].append(val)

                label = "avg_rtt_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                if len(self.r2) != 0:
                    val = round((val_a2 / len(self.r2)), 2)
                else:
                    val = 0.0
                self.data[label].append(val)
            else:
                log.info("No rtt data, adding default values set to 0.0")
                label = "avg_rtt_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0.0)
                label = "avg_rtt_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0.0)
                label = "avg_loss_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0.0)
                label = "avg_loss_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0.0)
                #label = "#robots"
                #if label in self.data.keys():
                #    if len(self.data[label]) == self.batch_size:
                #        del self.data[label][0]
                #else:
                #    self.data[label] = []
                #self.data[label].append(0)
                label = "r_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0)
                label = "r_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(0)
                log.info("no robot detected, reading only cpu")
                for label in self.data.keys():
                    log.debug("{}->{}".format(label, self.data[label]))
                self.set_temp['node_cpu_seconds_total'] = False
                self.set_temp['cmd_sent'] = False
                self.set_temp['cmd_lost'] = False
                self.set_temp['rtt_latency'] = False
                if self.save:
                    self.savedata()
                if self.producer is not None:
                    msg = self.create_json()
                    #self.kafka_send(str(self.get_forecasting_value(self.back, self.forward)))
                    self.kafka_send(msg)
                return
            '''
            if 'upv' in temp1.keys():
                avg_up = sum(temp1['upv']) / len(temp1['upv'])
                string = string + ";" + str(round(avg_up, 2))

            '''
            if 'comlv' in temp1.keys() and 'comsv' in temp1.keys():
                lv_a1 = 0.0
                lv_a2 = 0.0
                sv_a1 = 0.0
                sv_a2 = 0.0

                # avg_coml = sum(temp1['comlv']) / len(temp1['comlv'])
                # avg_coms = sum(temp1['comsv']) / len(temp1['comsv'])
                for i in range(0, len(temp1['comlv'])):
                    lv = int(temp1['comlv'][i])
                    sv = 0
                    if i < len(temp1['comsv']):
                        sv = int(temp1['comsv'][i])
                    if sv != 0:
                        if temp1['coml_id'][i] in self.r1:
                            lv_a1 = lv_a1 + lv
                            sv_a1 = sv_a1 + sv
                        elif temp1['coml_id'][i] in self.r2:
                            lv_a2 = lv_a2 + lv
                            sv_a2 = sv_a2 + sv
                        elif temp1['coml_id'][i] in self.other_robs:
                            log.info("Robot handled {} by other jobs ".format(temp1['coml_id'][i]))
                if sv_a1 != 0:
                    l_a1 = round(float(lv_a1 / sv_a1), 5)
                else:
                    l_a1 = 0.0
                if sv_a2 != 0:
                    l_a2 = round(float(lv_a2 / sv_a2), 5)
                else:
                    l_a2 = 0.0
                label = "avg_loss_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                    elif len(self.data[label]) > self.batch_size:
                        diff = len(self.data[label]) - self.batch_size
                        log.info("::::ATTENTION:::Deleting {} samples from the list".format(diff))
                        for i in range(0, diff+1):
                            del self.data[label][i]
                else:
                    self.data[label] = []
                if len(self.r1) != 0:
                    val = round((l_a1 / len(self.r1)), 5)
                else:
                    val = 0.0
                self.data[label].append(val)
                label = "avg_loss_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                    elif len(self.data[label]) > self.batch_size:
                        diff = len(self.data[label]) - self.batch_size
                        log.info("::::ATTENTION:::Deleting {} samples from the list".format(diff))
                        for i in range(0, diff+1):
                            del self.data[label][i]
                else:
                    self.data[label] = []
                if len(self.r2) != 0:
                    val = round((l_a2 / len(self.r2)), 5)
                else:
                    val = 0.0
                self.data[label].append(val)
            else:
                self.data["avg_loss_a1"].append("0.0")
                self.data["avg_loss_a2"].append("0.0")
                log.info("no cmd lost detected")
            robs = self.r1 + self.r2
            if len(robs) != 0:
                #label = "#robots"
                #if label in self.data.keys():
                #    if len(self.data[label]) == self.batch_size:
                #        del self.data[label][0]
                #else:
                #    self.data[label] = []
                #self.data[label].append(len(robs))
                label = "r_a1"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(len(self.r1))
                log.info("{}->{}".format(label, self.data[label]))
                label = "r_a2"
                if label in self.data.keys():
                    if len(self.data[label]) == self.batch_size:
                        del self.data[label][0]
                else:
                    self.data[label] = []
                self.data[label].append(len(self.r2))
                log.info("{}->{}".format(label, self.data[label]))
            self.set_temp['node_cpu_seconds_total'] = False
            self.set_temp['cmd_sent'] = False
            self.set_temp['cmd_lost'] = False
            self.set_temp['rtt_latency'] = False
            if self.save:
                self.savedata()
            #if self.producer is not None:
            #    self.kafka_send(str(self.get_forecasting_value(self.back, self.forward)))
            if self.producer is not None:
                msg = self.create_json()
                self.kafka_send(msg)

            self.temp = {}

    def kafka_send(self, message):
        def delivery_callback(err, msg):
            if err:
                log.error('%% Message failed delivery: %s\n' % err)
            else:
                log.debug('%% Message delivered to %s [%d] @ %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))

        # p.produce(topic, key="metric", value=message, callback=delivery_callback)
        self.producer.produce(self.outTopic, value=json.dumps(message).encode('utf-8'), callback=delivery_callback)
        self.producer.flush()
        self.producer.poll(1)

    def create_json(self):
        val = self.get_forecasting_value(self.back, self.forward)

        for elem in self.loaded_json:
            if elem['metric']['__name__'] == "node_cpu_seconds_total":
                elem['value'][1] = val
        print(val)
        print(self.loaded_json)
        return self.loaded_json


    def get_names(self):
        return self.names

    def run(self, event, consumer, avg):
        log.debug(self.instance_name + ": forecasting Job, starting the Kafka Consumer")
        while not event.is_set():
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log.error('Forecatsing Job: %% %s [%d] reached end at offset %d\n' % (msg.topic(),
                                                                                              msg.partition(),
                                                                                              msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # no error -> message received
                    if self.model == "lstmCPUEnhanced":
                        if self.producer is not None:
                            self.loaded_json = json.loads(msg.value())
                            print(json.dumps(self.loaded_json, indent=4, sort_keys=True))
                        self.data_parser2(msg.value())
                    else:
                        self.data_parser1(msg.value(), avg)

            except ConsumeError as e:
                log.error(self.instance_name + ": forecasting Job Consumer error: {}".format(str(e)))
                # Should be commits manually handled?
                consumer.close()

    def str(self):
        return '{ Forecasting Job:\n\tmodel: ' + str(self.model) + '\n\tjob_id: ' + str(self.job_id) + \
               '\n\tns_type: ' + str(self.nstype) + 'instance_name: ' + self.instance_name + \
               '\n\ttime_steps: ' + str(self.time_steps) + '\n\tbatch_size: ' + str(self.batch_size) + '\n}'

    def set_data(self, data):
        self.data = data

    def add_data(self, data):
        if len(data) == self.time_steps:
            self.data = data
        elif len(data) > self.time_steps:
            self.data = data[-self.time_steps:]
        else:
            init = self.time_steps - len(data)
            temp = self.data[-init:]
            self.data = np.concatenate((temp, data), axis=0)

    def set_model(self, back, forward, load, loadfilename, save, features=None, m_feature=None, savefilename=None):
        self.back = back
        self.forward = forward
        if savefilename is None:
            savefilename = "trainedModels/defaultModel.h5"
        if features is not None:
            self.features = features
        if m_feature is not None:
            self.main_feature = m_feature
        for feat in self.features:
            self.set_temp[feat] = False
        #fix for label naming
        self.set_temp['node_cpu_seconds_total'] = False
        self.set_temp['rtt_latency'] = False
        self.set_temp['cmd_sent'] = False
        self.set_temp['cmd_lost'] = False

        if self.model == "lstmCPUBase":
            if load:
                self.load_lstmcpubase(back, forward, loadfilename)
            else:
                self.train_model1(0.8, back, forward, None, savefilename)
        if self.model == "lstmRAMEnhanced":
            if load:
                self.load_model_dt(back, forward, loadfilename)
            else:
                self.train_model_dt(800, loadfilename, savefilename)
            #save collected data on csb file
            if save:
                self.save = True
                #csv file initialization and header drop
                csv_file = open(self.csv, "w")
                listx = []
                listx.append(self.main_feature)
                listx = listx + self.features
                string = ""
                for m in listx:
                    string = string + str(m) + ";"
                string = string[:-1] + "\n"
                csv_file.write(string)
                csv_file.close()

    def get_forecasting_value(self, n_features, desired):
        if self.model == "Test":
            return round(float(np.sum(self.data.astype(np.float)) / len(self.data)), 2)
        elif self.model == "lstmCPUBase":
            data1 = copy.deepcopy(self.datalist)
            datax = data1[-1]
            log.debug("Last data in the list is: {}".format(datax.getvalue()))
            # df = pd.read_csv(self.lstm_data, sep=";")
            df = pd.read_csv(datax, sep=";")
            ds = df.values
            scaler = MinMaxScaler(feature_range=(0, 1))
            dsx = scaler.fit_transform(ds)
            testx = dsx
            test = np.reshape(testx, (1, 1, n_features))
            value = self.trained_model.predict(desired, test, scaler, n_features)
            return round(float(value[0]), 2)
        elif self.model == "lstmCPUEnhanced":
            log.debug(self.instance_name + ": forecasting value for lstmCPUEnhanced instance ")
            if len(self.data.keys()) == 0:
                log.debug("No data in the db")
                return 0.0
            else:
                for label in self.data.keys():
                    if len(self.data[label]) < self.back:
                        if "#robots" not in label:
                            log.debug(self.instance_name + ": feature {} has low number of samples ".format(label))
                            return 0.0
            log.info("TESTING: data samples \n{} ".format(self.data))
            value = self.trained_model.predict(self.data)
            if len(value) == 1:
                log.info("TESTING: T0 is {}, value is {} ".format(self.t0[self.main_feature], value[0]))
                return round((float(value[0]) + self.t0[self.main_feature]), 2)
            if len(value) > 1:
                temp = []
                for val in value:
                    temp.append(float(val) + float(self.t0[self.main_feature]))
                return round(float(value[self.forward - 1]), 2)

            else:
                return 0.0
        else:
            return 0.0

    def is_forecasting(self):
        return self.forecast

    def get_model(self):
        return self.model

    def set_trained_model(self, model):
        self.trained_model = model

    def load_lstmcpubase(self, back, forward, filename):
        log.debug(self.instance_name + ": forecasting Job, loading the LSTM base forecasting model")
        lstm = lstmcpu(None, None, back, forward, None)
        lstm.load_trained_model(filename)
        self.set_trained_model(lstm)
        return self.trained_model

    def load_model_dt(self, back, forward, filename):
        log.debug(self.instance_name + ": forecasting Job, loading the LSTM enhanced forecasting model")

        lstm = lstmramdt(None, None, back, forward, None, self.features, self.main_feature)

        lstm.load_trained_model(filename)
        self.set_trained_model(lstm)
        return self.trained_model

    def train_model1(self, ratio, back, forward, data_file, model_file):
        if data_file is None:
            data_file = "../data/example-fin.csv"
        lstm = lstmcpu(data_file, ratio, back, forward, 0.90)
        lstm.get_dataset(True, 0, 1)
        lstm.split_sequences_train()
        lstm.reshape()
        lstm.train_lstm(True, model_file)
        self.set_trained_model(lstm)
        return self.trained_model

    def train_model_dt(self, split, load_file, save_file):
        if load_file is None:
            load_file = "data/dataset_train.csv"
        lstm = lstmramdt(load_file, split, self.back, self.forward, 0.90, self.features, self.main_feature)
        lstm.train(save=True, filename=save_file, split=split)
        self.set_trained_model(lstm)
        return self.trained_model

    def get_robots(self):
        return self.r1 + self.r2

    def set_update_robots(self, val):
        self.update_robots = val

    def set_other_robots(self, val):
        self.other_robs = val

    def config_t0(self, lab, val):
        self.set_t0[lab] = True
        self.t0[lab] = val
