##################################
## prepare data
#################################
library(tidyverse)
library(data.table)
library(lubridate)
library(readr)


options(datatable.optimize=2L)
## create orderSales only prod
setkey(orderSales,skuId)
setkey(prod,skuId)
orderSales <- merge(orderSales,prod[,.(skuId)])

##orderSales <- orderSales[date < "2019-04-01"]
## create OrderSales without возвратов
## особенность внутренних заказов, что когда пришли их списывают через минус
orderSales <- orderSales[!(internal == "Да" & order <0)]
orderSales <- orderSales[,.(order = sum(order),orderRub = sum(orderRub))
                         ,.(idCheck,internal,cardId,skuId,shopId,storeId,date)]

orderSales[date >= "2019-04-01" & date <= "2019-04-30",sum(order)]

## тут нужно понимать, что делать с внешними заказами при возврате?, по идее материал нужен и под них

## way 1 without minus если от заказа отказались, то отказались
## фактически тут чистые продажи внешнии и  внутренние
## округляем до месяца, т.к. прогноз на месяц вперед
##orderSales$mDate <- floor_date(orderSales$date, "month")

##h<-orderSales[,.(order = sum(order),orderRub = sum(orderRub)),date]

##create specification only prod
##setkey(specification,skuId)
##setkey(prod,skuId)
##specification <- merge(specification,prod[,.(skuId)])

##create stockGP only prod
##setkey(stockGP,skuId)
##setkey(prod,skuId)
##stockGP <- merge(stockGP,prod[,.(skuId)])


## create specification two data: startDate and finishDate
temp <- specification[,.N,.(skuId,date)][order(skuId,date)]
temp[order(skuId,date),("lagDate"):= shift(date,1,NA,"lead"),by = c("skuId")]
temp[is.na(temp)] <- ymd_hms("2019-12-01 00:00:00")
temp[,idSpec:= seq(1,dim(temp)[1])]

setkey(specification,skuId,date)
setkey(temp,skuId,date)

specification<- merge(specification,temp[,.(skuId,date,lagDate,idSpec)])
names(specification)[2] <- "startDate"
names(specification)[6] <- "finishDate"
specification[,specificationId:=NULL]
specification <- specification[,.(idSpec,skuId,startDate,finishDate,materialId,qnt)]
specification[,minD:= min(startDate),.(skuId)]
specification[startDate == minD]$startDate <- ymd_hms("2017-01-01 00:00:00")
specification[,minD:=NULL]


#####################
## распутываем данные по материалами
#####################

## step_01
temp <- specification[,.(idSpec = min(idSpec)),.(skuId,startDate,finishDate)]
setkey(orderSales,skuId)
setkey(temp,skuId)
h <- merge(orderSales,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate)]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T)
h$materialId_01 <- h$materialId
names(h)[2] <- c("orderSkuId")
names(h)[11] <- c("skuId")
names(h)[12] <- c("qnt_01")
h[,idSpec:=NULL]

##step_02
setkey(h,skuId)
setkey(temp,skuId)
h <- merge(h,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate) | (is.na(idSpec))]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T,all.x = T)
names(h)[15] <- c("qnt_02")
h$materialId_02 <- h$materialId
h[,idSpec:=NULL]
h[,skuId:=NULL]
names(h)[12] <- c("skuId")

##step_03
setkey(h,skuId)
setkey(temp,skuId)
h <- merge(h,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate) | (is.na(idSpec))]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T,all.x = T)
names(h)[17] <- c("qnt_03")
h$materialId_03 <- h$materialId
h[,idSpec:=NULL]
h[,skuId:=NULL]
names(h)[14] <- c("skuId")

##step_04
setkey(h,skuId)
setkey(temp,skuId)
h <- merge(h,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate) | (is.na(idSpec))]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T,all.x = T)
names(h)[19] <- c("qnt_04")
h$materialId_04 <- h$materialId
h[,idSpec:=NULL]
h[,skuId:=NULL]
names(h)[16] <- c("skuId")

##step_05
setkey(h,skuId)
setkey(temp,skuId)
h <- merge(h,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate) | (is.na(idSpec))]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T,all.x = T)
names(h)[21] <- c("qnt_05")
h$materialId_05 <- h$materialId
h[,idSpec:=NULL]
h[,skuId:=NULL]
names(h)[18] <- c("skuId")

##step_06
setkey(h,skuId)
setkey(temp,skuId)
h <- merge(h,temp,allow.cartesian = T,all.x = T)
h <- h[(date >= startDate & date < finishDate) | (is.na(idSpec))]
h[,startDate:=NULL]
h[,finishDate:=NULL]

setkey(specification,idSpec)
setkey(h,idSpec)
h<- merge(h,specification[,.(idSpec,materialId,qnt)],allow.cartesian = T,all.x = T)

## create final material for matras
h$materialId <- h$materialId_05
h$qnt <- h$qnt_05

h$materialId <- ifelse(is.na(h$materialId),h$materialId_04,h$materialId)
h$qnt <- ifelse(is.na(h$qnt),h$qnt_04,h$qnt)

h$materialId <- ifelse(is.na(h$materialId),h$materialId_03,h$materialId)
h$qnt <- ifelse(is.na(h$qnt),h$qnt_03,h$qnt)

h$materialId <- ifelse(is.na(h$materialId),h$materialId_02,h$materialId)
h$qnt <- ifelse(is.na(h$qnt),h$qnt_02,h$qnt)

h$materialId <- ifelse(is.na(h$materialId),h$materialId_01,h$materialId)
h$qnt <- ifelse(is.na(h$qnt),h$qnt_01,h$qnt)




## check data with rowProd
setkey(h,materialId)
setkey(rowProd,materialId)
temp <- merge(h,rowProd[,.(materialId,level1,level2,level3,level4,level5)])
h[,.N,materialId][,.N]

temp <- temp[level3 %in% c("1.5.3 Ткань трикотажная","1.5.5 Лазурит","1.5.2 Ткань жаккардовая"
                   ,"1.18.1 Листовой ППУ","1.18.2 Рулонный ППУ","1.5.4 Бязь"
                   ,"1.5.7 Ткань Прочее","Независимые пружинные блоки (EVS)"
                   ,"Зависимые пружинные блоки (Bonnell)")]

temp[,.N,materialId][,.N]


## create demand on material
temp[,order_01:= qnt_01*order]
temp[,order_02:= qnt_02*order_01]
temp[,order_03:= qnt_03*order_02]
temp[,order_04:= qnt_04*order_03]
temp[,order_05:= qnt_05*order_04]

temp$orderMaterial <- temp$order_05 
temp$orderMaterial <- ifelse(is.na(temp$orderMaterial),temp$order_04,temp$orderMaterial)
temp$orderMaterial <- ifelse(is.na(temp$orderMaterial),temp$order_03,temp$orderMaterial)
temp$orderMaterial <- ifelse(is.na(temp$orderMaterial),temp$order_02,temp$orderMaterial)
temp$orderMaterial <- ifelse(is.na(temp$orderMaterial),temp$order_01,temp$orderMaterial)

##r <- temp[materialId == "206831a0-63c7-11e2-ad35-ac162d78f2b8"]
##fwrite(r,"r2.csv")
#################################
## aligthment with rowStock
################################
rowStock$date <- ymd(rowStock$date)
rowStock <- rowStock[materialId %in% temp[,.N,materialId]$materialId]

minDate <- min(rowStock$date)
maxDate <- max(rowStock$date)
calendar <- data.table(seq(minDate,maxDate,by = "day"))
names(calendar) <- c("date")

## agrigate rowData to all StoreId

rowStock[order(materialId,storeId,date),("lagDate"):= shift(date,1,NA,"lead"),by = c("materialId","storeId")]
rowStock$lagDate[is.na(rowStock$lagDate)] <- ymd("2019-12-01")
rowStock <- rowStock[,.(materialId,storeId,date,lagDate,rowStock,rowReserve)]
names(rowStock)[3] <- c("startDate")
names(rowStock)[4] <- c("finishDate")


calendarMaterialIdStoreId <- setkey(rowStock[,.N,.(materialId,storeId)
                                             ][,.(materialId,storeId)
                                               ][,c(k=1,.SD)],k)[calendar[,c(k=1,.SD)
                                                                          ],allow.cartesian=TRUE][,k:=NULL]


setkey(calendarMaterialIdStoreId,materialId,storeId)
setkey(rowStock,materialId,storeId)

rowStockP <- merge(calendarMaterialIdStoreId,rowStock,allow.cartesian=TRUE,all.x = T)
rowStockP <- rowStockP[date >= startDate & date < finishDate]
rowStockP <- rowStockP[order(materialId,storeId,date)]
rowStockP[,startDate:= NULL]
rowStockP[,finishDate:= NULL]

#################################
## aligthment with stockGP
################################
stockGP$date <- ymd(stockGP$date)
stockGP <- stockGP[skuId %in% orderSales[,.N,skuId]$skuId]

stockGP1 <- stockGP[date < "2018-07-01"]
stockGP2 <- stockGP[date >= "2018-07-01"]

stockGP <- stockGP1
###stockGP
minDate <- min(stockGP$date)
maxDate <- max(stockGP$date)
calendar <- data.table(seq(minDate,maxDate,by = "day"))
names(calendar) <- c("date")

## agrigate rowData to all StoreId

stockGP[order(skuId,storeId,date),("lagDate"):= shift(date,1,NA,"lead"),by = c("skuId","storeId")]
stockGP$lagDate[is.na(stockGP$lagDate)] <- ymd("2019-12-01")
stockGP <- stockGP[,.(skuId,storeId,date,lagDate,stockGP,reserveGP)]
names(stockGP)[3] <- c("startDate")
names(stockGP)[4] <- c("finishDate")


calendarMaterialIdStoreId <- setkey(stockGP[,.N,.(skuId,storeId)
                                             ][,.(skuId,storeId)
                                               ][,c(k=1,.SD)],k)[calendar[,c(k=1,.SD)
                                                                          ],allow.cartesian=TRUE][,k:=NULL]

##rm(calendar,h,orderSales,rowStock,specification)

setkey(calendarMaterialIdStoreId,skuId,storeId)
setkey(stockGP,skuId,storeId)

stockGPA1 <- merge(calendarMaterialIdStoreId,stockGP,allow.cartesian=TRUE,all.x = T)
stockGPA1 <- stockGPA1[date >= startDate & date < finishDate]
stockGPA1 <- stockGPA1[order(skuId,storeId,date)]
stockGPA1[,startDate:= NULL]
stockGPA1[,finishDate:= NULL]


stockGP <- stockGP2
###stockGP
minDate <- min(stockGP$date)
maxDate <- max(stockGP$date)
calendar <- data.table(seq(minDate,maxDate,by = "day"))
names(calendar) <- c("date")

## agrigate rowData to all StoreId

stockGP[order(skuId,storeId,date),("lagDate"):= shift(date,1,NA,"lead"),by = c("skuId","storeId")]
stockGP$lagDate[is.na(stockGP$lagDate)] <- ymd("2019-12-01")
stockGP <- stockGP[,.(skuId,storeId,date,lagDate,stockGP,reserveGP)]
names(stockGP)[3] <- c("startDate")
names(stockGP)[4] <- c("finishDate")


calendarMaterialIdStoreId <- setkey(stockGP[,.N,.(skuId,storeId)
                                            ][,.(skuId,storeId)
                                              ][,c(k=1,.SD)],k)[calendar[,c(k=1,.SD)
                                                                         ],allow.cartesian=TRUE][,k:=NULL]



setkey(calendarMaterialIdStoreId,skuId,storeId)
setkey(stockGP,skuId,storeId)

stockGPA2 <- merge(calendarMaterialIdStoreId,stockGP,allow.cartesian=TRUE,all.x = T)
stockGPA2 <- stockGPA2[date >= startDate & date < finishDate]
stockGPA2 <- stockGPA2[order(skuId,storeId,date)]
stockGPA2[,startDate:= NULL]
stockGPA2[,finishDate:= NULL]

stockGPA <- rbind(stockGPA1,stockGPA2)


