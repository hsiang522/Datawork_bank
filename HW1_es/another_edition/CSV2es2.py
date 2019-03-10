from pyspark import SparkContext
import json
sc = SparkContext.getOrCreate()
conf = {"es.nodes": "localhost" ,"es.port" : "9200","es.resource": "index_bank2/type_bank2","es.mapping.id": "doc_id","es.input.json": "true"}

class Main:
	def __init__(self):
		print("in init")	

	def handle_date(self,da):
    		if len(da) == 7:
        		return (str(int(da[:3])+1911)+"-"+da[3:5]+"-"+da[5:])
    		if len(da) == 6:
        		da = "0"+da
        		return (str(int(da[:3])+1911)+"-"+da[3:5]+"-"+da[5:])
    		if len(da) == 0:
        		return "1111-11-11"#define null	

	def main(self):
		#分別讀取五個檔案
		f1 = sc.textFile("file:/Users/mac/a_lvr_land_a.csv")
		f2 = sc.textFile("file:/Users/mac/b_lvr_land_a.csv")
		f3 = sc.textFile("file:/Users/mac/e_lvr_land_a.csv")
		f4 = sc.textFile("file:/Users/mac/f_lvr_land_a.csv")
		f5 = sc.textFile("file:/Users/mac/h_lvr_land_a.csv")
		
		f_old=[f1,f2,f3,f4,f5]	
		f_new=[]	
		#將資料title去掉
		for i in range(5):
			header = sc.parallelize(f_old[i].take(2))
			f_new.append(f_old[i].subtract(header))
		#逗號分格每一個欄位並整合五份數據成一個f_all
		f_new[0] = f_new[0].map(lambda line:[line.split(",")[:],"臺北市"])
		f_new[1] = f_new[1].map(lambda line:[line.split(",")[:],"臺中市"])
		f_new[2] = f_new[2].map(lambda line:[line.split(",")[:],"高雄市"])
		f_new[3] = f_new[3].map(lambda line:[line.split(",")[:],"新北市"])
		f_new[4] = f_new[4].map(lambda line:[line.split(",")[:],"桃園市"])
		f_all=f_new[0].union(f_new[1]).union(f_new[2]).union(f_new[3]).union(f_new[4])
		
		#將時間轉換成yyyy-mm-dd格式
		rd_rt = f_all.map(lambda x: [x[0][:7],str(int(x[0][7][:3])+1911)+"-"+x[0][7][3:5]+"-"+x[0][7][5:],x[0][8:],x[1]])
		rd_rtz = rd_rt.zipWithIndex()
		#將格式轉換成可上傳es的格式，並利用int(x or 0)避免空值錯誤，接著上傳
		rd_rt_all = rd_rtz.map(lambda g:(str(g[1]),json.dumps({'doc_id':str(g[1]),'city':g[0][3],'date':g[0][1],\
			'urban_district': g[0][0][0], 'transaction_sign': g[0][0][1],'position': g[0][0][2],\
			'area': float(g[0][0][3]or 0), 'district': g[0][0][4],'land_use_district': g[0][0][5],\
			'land_use': g[0][0][6],'transaction_pen': g[0][2][0],'shifting_level': g[0][2][1],\
			'total_floor': g[0][2][2],'building_state': g[0][2][3],'main_use': g[0][2][4],\
			'building_materials': g[0][2][5],'complete_date': self.handle_date(g[0][2][6]),'shifting_area': float(g[0][2][7]or 0),\
			'room': int(g[0][2][8]or 0),'hall': int(g[0][2][9]or 0),'health': int(g[0][2][10]or 0),\
			'compartmented': g[0][2][11],'organization': g[0][2][12],'total_price': int(g[0][2][13]or 0),\
			'unit_price': int(g[0][2][14]or 0),'berth': g[0][2][15],'berth_total_area': float(g[0][2][16]or 0),\
			'berth_total_price': int(g[0][2][17]or 0),'note': g[0][2][18],'serial_number': g[0][2][19]},ensure_ascii=False)))
		rd_rt_all.saveAsNewAPIHadoopFile(
		path='-',
		outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
		keyClass="org.apache.hadoop.io.NullWritable",
		valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
		conf=conf)	

if __name__ == "__main__":
 # if you call this script from the command line (the shell) it will
 # run the 'main' function
 main()
