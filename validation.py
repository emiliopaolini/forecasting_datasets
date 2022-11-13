from tools.Classes import ForecastingJob
from tools.externalConnections import ExternalConnections
from threading import Thread, Event
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.error import ConsumeError
import sys
import joblib
import time
import matplotlib.pyplot as plt


il = 1
load = test = plot = False

diff = True
if load is True:
    save = False
else:
    save = True

modelFile = 'trainedModels/lstm5sec.h5'
#modelFile = 'trainedModels/lstm_120_4.h5'
dataload = 'data/data_RAM_train.csv'
traindata = 'data/data_RAM_train.csv'

fj = ForecastingJob("test", "test", "lstmRAMEnhanced", "node_cpu_seconds_total", il, "dtcontrolvnf-1", outTopic=None,
                    output=None)
#input and output features
steps_back = 130 #130 to include two peaks
steps_forw = 1
if load:
    # input model file to be loaded (no train)
    #loadfile = 'trainedModels/lstmdiff' + str(steps_back) + '_' + str(steps_forw) + '.h5'
    loadfile = modelFile
else:
    #dataset to be used for training in case load is False
    #loadfile = 'data/dataset_train.csv'
    loadfile = traindata
#output model file
savefile = modelFile
features = []
#features = ['avg_rtt_a1', 'avg_rtt_a2', 'avg_loss_a1', 'avg_loss_a2', 'r_a1', 'r_a2']
#features = ['avg_rtt_a1', 'avg_rtt_a2', 'r_a1', 'r_a2']
features = ['r_a1', 'r_a2']
main_feature = 'memory_free'

fj.set_model(steps_back, steps_forw, load, loadfile, save, features, main_feature, savefile)
file2 = open('data/comparison.csv', "w")
file2.write("{};{};{}\n".format("t0", "t+4", "forecasting"))




'''
if test:
    data ={}
    db ={}

    filex = open(dataload, "r")
    a = filex.readlines()
    filex.close()
    db['cpu0'] =[]
    db['avg_rtt_a1'] = []
    db['avg_rtt_a2'] = []
    db['avg_loss_a1'] = []
    db['avg_loss_a2'] = []
    db['r_a1'] = []
    db['r_a2'] = []
    for row in range(0, len(a)):
        line = a[row].split(';')
        db['cpu0'].append(round(float(line[0]), 1))
        db['avg_rtt_a1'].append(round(float(line[1]), 1))
        db['avg_rtt_a2'].append(round(float(line[2]), 1))
        db['avg_loss_a1'].append(round(float(line[3]), 1))
        db['avg_loss_a2'].append(round(float(line[4]), 1))
        db['r_a1'].append(round(float(line[5]), 1))
        db['r_a2'].append(round(float(line[6]), 1))
    #print("read all data {}".format(db))

    start = 0
    setT0 = True
    stay = True
    t0 = 0
    while(stay):
        end = start + steps_back
        fval = end + 4
        if fval == len(a):
            stay = False
        print("reading data form {} to {} and future instant {}".format(start, end, fval))
        data['cpu0'] = []
        data['avg_rtt_a1'] = []
        data['avg_rtt_a2'] = []
        data['avg_loss_a1'] = []
        data['avg_loss_a2'] = []
        data['r_a1'] = []
        data['r_a2'] = []
        for row in range(start, end):
            line = a[row].split(';')
            data['cpu0'].append(round(float(line[0]), 1))
            data['avg_rtt_a1'].append(round(float(line[1]), 1))
            data['avg_rtt_a2'].append(round(float(line[2]), 1))
            data['avg_loss_a1'].append(round(float(line[3]), 1))
            data['avg_loss_a2'].append(round(float(line[4]), 1))
            data['r_a1'].append(round(float(line[5]), 1))
            data['r_a2'].append(round(float(line[6]), 1))
        print("start={}\tend={}\tfuture={}".format(db['cpu0'][start], db['cpu0'][end-1], db['cpu0'][fval-1]))
        if setT0:
            t0 = data['cpu0'][0]
            fj.config_t0(main_feature, t0)
            setT0 = False
        fj.set_data(data)
        if diff:
            value = fj.get_forecasting_value(10, 1)-t0
        else:
            value = fj.get_forecasting_value(10, 1)
        fut = db['cpu0'][fval-1]
        #mmscaler_cpu = joblib.load("trainedModels/lstm_mmscaler_cpu")
        #print(mmscaler_cpu.transform([[fut]]))
        print("t0={}\tT+4={}\tforecast={}".format(t0, fut, value))
        start = start + 1
        file2.write("{};{};{} \n".format(db[main_feature][end-1], fut, value))
file2.close()

if plot:
    data = {}
    db = {}

    filex = open('data/comparison.csv', "r")
    a = filex.readlines()
    filex.close()
    db['T'] = []
    db['T+4'] = []
    db['forecasting'] = []
    for row in range(1, len(a)):
        line = a[row].split(';')
        db['T'].append(round(float(line[0]), 1))
        db['T+4'].append(round(float(line[1]), 1))
        db['forecasting'].append(round(float(line[2]), 1))

    plt.plot(db['T'])
    plt.plot(db['T+4'])
    plt.plot(db['forecasting'])
    plt.title('comparison')
    plt.ylabel('cpu')
    plt.xlabel('time')
    plt.legend(['T', 'T+4', 'forecasting'], loc='upper left')
    plt.show()
    plt.figure(figsize=(20, 4))
'''