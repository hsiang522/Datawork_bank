from pyspark import SparkContext
import json
sc = SparkContext.getOrCreate()

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
        		return ""#define null
	
	
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

		#隨機分切成兩組
		rd_random=f_all.randomSplit([0.5,0.5])
		
		#開始將兩組依時間順序轉成json，將時間依倒序排列並轉換成yyyy-mm-dd，接著將檔案格式轉換成規定的time_slots格式，最後依序將檔案依序取名並存成json檔
		for i in range(2):
			rd_rt = rd_random[i].sortBy(lambda a: a[0][7], ascending=False)
			rd_rt2 = rd_rt.map(lambda x: [x[0][:7],str(int(x[0][7][:3])+1911)+"-"+x[0][7][3:5]+"-"+x[0][7][5:],x[0][8:],x[1]])
			rd_rt_all=rd_rt2.map(lambda g:{'city': g[3], 'time_slots': [{'date': g[1],'event':[{'urban_district': g[0][0],\
			    'transaction_sign': g[0][1],'position': g[0][2], 'area': float(g[0][3]or 0), 'district': g[0][4],\
				'land_use_district': g[0][5],'land_use': g[0][6],'transaction_pen': g[2][0],\
				'shifting_level': g[2][1],'total_floor': g[2][2],'building_state': g[2][3],'main_use': g[2][4],\
				'building_materials': g[2][5],'complete_date': self.handle_date(g[2][6]),'shifting_area': float(g[2][7]or 0),\
				'room': int(g[2][8]or 0),'hall': int(g[2][9]or 0),'health': int(g[2][10]or 0),\
				'compartmented': g[2][11],'organization': g[2][12],'total_price': int(g[2][13]or 0),\
				'unit_price': int(g[2][14]or 0),'berth': g[2][15],'berth_total_area': float(g[2][16]or 0),\
				'berth_total_price': int(g[2][17]or 0),'note': g[2][18],'serial_number': g[2][19]}]}]})
				
			data_str = rd_rt_all.map(lambda x: json.dumps(x,ensure_ascii=False))
			name = "result-part" + str(i+1) + ".json"
			data_str.coalesce(1).saveAsTextFile('/Users/mac/'+ name)

if __name__ == "__main__":
 # if you call this script from the command line (the shell) it will
 # run the 'main' function
 main()
