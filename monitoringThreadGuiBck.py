from pysnmp.hlapi import *
#import thread
import time
import os, sys, threading, time
import datetime
import influxdb



ipdb='172.17.0.2'
#ipdb='localhost'
gui=1   
test=0

'''

tx-rate=.1.3.6.1.4.1.14988.1.1.1.1.1.2.4
rx-rate=.1.3.6.1.4.1.14988.1.1.1.1.1.3.4
strength=.1.3.6.1.4.1.14988.1.1.1.1.1.4.4
ssid=.1.3.6.1.4.1.14988.1.1.1.1.1.5.4
bssid=.1.3.6.1.4.1.14988.1.1.1.1.1.6.4
frequency=.1.3.6.1.4.1.14988.1.1.1.1.1.7.4
band=.1.3.6.1.4.1.14988.1.1.1.1.1.8.4
tx-rate=.1.3.6.1.4.1.14988.1.1.1.3.1.2.4
rx-rate=.1.3.6.1.4.1.14988.1.1.1.3.1.3.4
ssid=.1.3.6.1.4.1.14988.1.1.1.3.1.4.4
bssid=.1.3.6.1.4.1.14988.1.1.1.3.1.5.4
client-count=.1.3.6.1.4.1.14988.1.1.1.3.1.6.4
frequency=.1.3.6.1.4.1.14988.1.1.1.3.1.7.4
band=.1.3.6.1.4.1.14988.1.1.1.3.1.8.4
noise-floor=.1.3.6.1.4.1.14988.1.1.1.3.1.9.4
overall-ccq=.1.3.6.1.4.1.14988.1.1.1.3.1.10.4'''


stop_threads = False
        
#keys=[1,2,3,4,5]
keys=[1,2,3,4,5]

devices={}
devices[0]="172.17.0.100"
devices[1]="172.17.0.101"
devices[2]="172.17.0.102"
devices[3]="172.17.0.103"
devices[4]="172.17.0.104"
devices[5]="172.17.0.105"
devices[6]="172.17.0.106"


d={}
d[0]="car"
d[1]="Door1"
d[2]="Door2"
d[3]="Door3"
d[4]="Door4"
d[5]="Door5"
d[6]="Door6"

def query(key, stop ):
   global d
   input_file = open("monitoring"+d[key]+".txt","a")
   while True:   
      millis = int(round(time.time() * 1000))
      #test
      if test==1:
            g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget(('demo.snmplabs.com', 161)),
                 ContextData(),
                 
                 ObjectType(ObjectIdentity('1.3.6.1.2.1.1.6.0')),
                 ObjectType(ObjectIdentity('1.3.6.1.2.1.1.4.0'))
            )
      #with door equipment
      else:      
           if key==0:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.3.1.6.4')),
              )
           #door2
           elif key==2:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 #TX                         
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.2.10')),
                 #RX
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.3.10')),
                 #RSSI
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.4.10')),
              )        
           #door3
           elif key==3:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 #TX                         
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.2.7')),
                 #RX
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.3.7')),
                 #RSSI
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.4.7')),
              )
           elif key==5:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 #TX                         
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.2.6')),
                 #RX
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.3.6')),
                 #RSSI
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.4.6')),
              )           
           elif key==1:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 #TX                         
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.2.9')),
                 #RX
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.3.9')),
                 #RSSI
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.4.9')),
              )


              
           #door1,4
           else:
              g = getCmd(SnmpEngine(),
                 CommunityData('public'),
                 UdpTransportTarget((devices[key], 161)),
                 ContextData(),
                 #TX                         
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.2.8')),
                 #RX
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.3.8')),
                 #RSSI
                 ObjectType(ObjectIdentity('.1.3.6.1.4.1.14988.1.1.1.1.1.4.8')),
              )

            
      
      errorIndication, errorStatus, errorIndex, varBinds= next(g)
      if len(varBinds)>0:
         if test==0:
               if key==0:
                  [oid1, clients] = str(varBinds[0]).split("=", 1)
                  msg=str(millis)+"\t"+clients
                  input_file.write(msg+"\n")
                  print(d[key]+"\t"+msg)
               else:
                  #print "###"+str(varBinds[0])+"###" 
                  [oid1, tx] = str(varBinds[0]).split("=", 1)
                  #print "###"+str(varBinds[1])+"###"  
                  [oid2, rx] = str(varBinds[1]).split("=", 1)
                  #print "###"+str(varBinds[2])+"###"  
                  [oid3, rssi] = str(varBinds[2]).split("=", 1)
                  msg=str(millis)+"\t"+tx+"\t"+rx+"\t"+rssi
                  input_file.write(msg+"\n")
                  print(devices[key]+"\t"+msg)
                  if gui==1:
                     d = datetime.datetime.utcnow()
                     ts = int(((d - epoch).total_seconds())*1000)
                     json=[{'measurement': 'measurement',
                      'tags': {"devId": "door"+str(key)},
                      #'tags': {"devId": d[key]},
                      'time': ts,
                      'fields': {"txRate": int(tx), "rxRate": int(rx), "rssi": int(rssi)}
                     }]
                     client.write_points(json, time_precision='ms')
         else:
               [oid1, location] = str(varBinds[0]).split("=", 1)
               [oid2, contact] = str(varBinds[1]).split("=", 1)
               msg=str(millis)+"\t"+location.strip()+"\t"+contact.strip()+"\n"
               print(msg)
               input_file.write(msg)
      #else:
      #      print "len=0"
      varBinds=[]
      
      if stop(): 
         break
   input_file.close()

epoch = datetime.datetime(1970,1,1)

if gui==1:
   print "database not connected"
   client=influxdb.InfluxDBClient(ipdb, 8086, 'root', 'root', 'schindler')
   client.create_database('schindler')
   print "database created and connected"

k = int(raw_input('Insert device id (0=car; 1=door1, 2=door2, 10=all...): '))

   

loop_exit=0
threads={}
if k!=10:
   input_file = open("monitoring"+d[k]+".txt","w")
   msg="timestamp\t tx  \t  rx   \n"
   input_file.write(msg)
   input_file.close()
   threads[k] = threading.Thread(target = query, args =( k, lambda : stop_threads, )) 
   threads[k].start()

   while loop_exit==0:
     try:
        time.sleep(0.1)
     except KeyboardInterrupt:
        print("Ctrl-c received! Killing monitoring...")
        loop_exit=1
        stop_threads = True
        threads[k].join() 
        print('thread '+str(k)+' killed')
else:
    for key in keys:
      input_file = open("monitoring"+d[key]+".txt","w")
      if key!=0:
         msg="timestamp\t tx  \t  rx  \t rssi \n"
      else:
         msg="timestamp\t connected clients\n"
      input_file.write(msg)
      input_file.close()

      threads[key] = threading.Thread(target = query, args =( key, lambda : stop_threads, )) 
      threads[key].start()
          
    while loop_exit==0:
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            print("Ctrl-c received! Killing monitoring...")
            loop_exit=1
            stop_threads = True
            for keyz in keys:
               threads[keyz].join() 
               print('thread '+str(keyz)+' killed')
    
print('Closing the program...')
