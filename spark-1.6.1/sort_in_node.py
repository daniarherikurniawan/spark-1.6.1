from pyspark import SparkContext,SparkConf
import shutil,os, sys, time


# non-spark code
# if os.path.exists("../generated_file/result_py"):
#     shutil.rmtree("../generated_file/result_py")
if os.path.exists("/proj/cs331-uc/daniar/result_py"):
    shutil.rmtree("/proj/cs331-uc/daniar/result_py")

if os.path.exists("workfile"):
    os.remove("workfile")
    
#some of the configuration is set through declaration at spark-defaults.conf
conf = SparkConf().setAppName("Sorting").setMain("spark://node-1.testspark.cs331-uc.emulab.net:7077").set("spark.reducer.maxSizeInFlight", "1m")
# conf = SparkConf().setAppName("Sorting").setMain("spark://node-5.sparknewtopology.cs331-uc.emulab.net:7077")
# conf = SparkConf().setAppName("Sorting").setMain("spark://daniar-X450JF:7077")


sc = SparkContext(conf=conf)
# sc.setLogLevel("ALL")
text_file = sc.textFile("/proj/cs331-uc/daniar/SPARK/generated_file/list_int",2)
sorted_array = text_file.map(lambda a : (int(a),a)).sortByKey("true")

# print("start the delay. Inject TC!!")
# time.sleep(30)
# print("end the delay")

output = sorted_array.collect()
f = open('workfile', 'w')
for (num, unitcount) in output:
	f.write(str(num)+"\n")
    # print(num)
f.closed
# sorted_array.saveAsTextFile("/proj/cs331-uc/daniar/result_py")

# ./bin/spark-submit sort_in_node.py --main spark://daniar-X450JF:7077 --deploy-mode cluster --num-executors 4
