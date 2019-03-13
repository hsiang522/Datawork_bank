date這一欄的格式已轉成yyyy-mm-dd

而complete_date因為這一欄有較多的空值，所以我後來程式做了兩個版本，可供參考：  
1.原始版：  
保留原本的string格式。

2.another_edition：  
有值的話轉換成yyyy-mm-dd；然而es的date格式必須強制符合yyyy-mm-dd格式，而且單一數值不能為0，所以我將空值定義為1111-11-11。


以下為欄位對照表，左側為raw data欄位名稱，右側為輸出檔之欄位名稱：

鄉鎮市區 : urban_district  
交易標的 : transaction_sign  
土地區段位置建物區段門牌 : position  
土地移轉總面積平方公尺 : area  
都市土地使用分區 : district  
非都市土地使用分區 : land_use_district  
非都市土地使用編定 : land_use  
交易年月日 : date  
交易筆棟數 : transaction_pen  
移轉層次 : shifting_level  
總樓層數 : total_floor  
建物型態 : building_state  
主要用途 : main_use  
主要建材 : building_materials  
建築完成年月 : complete_date  
建物移轉總面積平方公尺 : shifting_area  
建物現況格局-房 : room  
建物現況格局-廳 : hall  
建物現況格局-衛 : health  
建物現況格局-隔間 : compartmented  
有無管理組織 : organization  
總價元 : total_price  
單價元平方公尺 : unit_price  
車位類別 : berth  
車位移轉總面積平方公尺 : berth_total_area  
車位總價元 : berth_total_price  
備註 : note  
編號 : serial_number  
