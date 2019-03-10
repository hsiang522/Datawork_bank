date這一欄的格式已轉成yyyy-mm-dd

而complete_date因為這一欄有較多的空值，所以我後來程式做了兩個版本，可供參考：  
1.原始版：  
保留原本的string格式。

2.another_edition：  
有值的話轉換成yyyy-mm-dd，然而es的date格式必須強制符合yyyy-mm-dd格式，而且不能單一數值不能為0，所以我將空值定義為1111-11-11。
