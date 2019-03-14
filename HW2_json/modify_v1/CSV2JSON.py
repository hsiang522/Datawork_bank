from pyspark import SparkContext
import json
sc = SparkContext.getOrCreate()


def main():
    # 分別讀取五個檔案
    f1 = sc.textFile("file:/Users/mac/a_lvr_land_a.csv")
    f2 = sc.textFile("file:/Users/mac/b_lvr_land_a.csv")
    f3 = sc.textFile("file:/Users/mac/e_lvr_land_a.csv")
    f4 = sc.textFile("file:/Users/mac/f_lvr_land_a.csv")
    f5 = sc.textFile("file:/Users/mac/h_lvr_land_a.csv")

    f_old = [f1, f2, f3, f4, f5]
    f_new = []

    for i in range(5):
        header = sc.parallelize(f_old[i].take(2))
        f_old[i] = f_old[i].subtract(header)  # 將資料title去掉
        f_old[i] = f_old[i].map(lambda line: [line.split(",")[:]])  # 將資料逗點分隔
        f_old[i] = f_old[i].map(lambda x: [x[0][:7], str(
            int(x[0][7][:3])+1911)+"-"+x[0][7][3:5]+"-"+x[0][7][5:], x[0][8:]])  # 處理時間格式
        f_old[i] = f_old[i].map(lambda g: (
            g[1], ([{"district": g[0][0], "building_state":g[2][3]}])))  # 準備reduceByKey的key
        f_old[i] = f_old[i].reduceByKey(lambda x, y: x+y)
        f_old[i] = f_old[i].sortBy(lambda a: a[0], ascending=False)  # sortBy時間
        f_old[i] = f_old[i].map(lambda x: {'date': x[0], 'events': x[1]})
        f_new.append(f_old[i])

    # 整合五份數據成一個f_all
    f_new[0] = f_new[0].map(lambda x: ("臺北市", [x]))
    f_new[1] = f_new[1].map(lambda x: ("臺中市", [x]))
    f_new[2] = f_new[2].map(lambda x: ("高雄市", [x]))
    f_new[3] = f_new[3].map(lambda x: ("新北市", [x]))
    f_new[4] = f_new[4].map(lambda x: ("桃園市", [x]))
    for i in range(5):
        f_new[i] = f_new[i].reduceByKey(
            lambda x, y: x+y)  # reduceByKey加入城市當key
        f_new[i] = f_new[i].map(lambda x: {'city': x[0], 'time_slot': x[1]})

    f_all = f_new[0].union(f_new[1]).union(
        f_new[2]).union(f_new[3]).union(f_new[4])
    # 隨機分切成兩組
    rd_random = f_all.randomSplit([0.5, 0.5])

    # 將兩組數據分別轉成json格式，最後依序將檔案取名並存成json檔
    for i in range(2):
        data_str = rd_random[i].map(
            lambda x: json.dumps(x, ensure_ascii=False))
        name = "result-part" + str(i+1) + ".json"
        data_str.coalesce(1).saveAsTextFile('/Users/mac/' + name)


if __name__ == "__main__":
    # if you call this script from the command line (the shell) it will
    # run the 'main' function
    main()
