##################################
## prepare data
#################################
library(tidyverse)
library(data.table)
library(lubridate)
library(readr)



## create orderSales only prod
setkey(orderSales,skuId)
setkey(prod,skuId)
orderSales <- merge(orderSales,prod[,.(skuId)])

## create OrderSales without возвратов
## особенность внутренних заказов, что когда пришли их списывают через минус
orderSales <- orderSales[!(internal == "Да" & order <0)]
orderSales <- orderSales[,.(order = sum(order),orderRub = sum(orderRub))
                         ,.(idCheck,internal,cardId,skuId,shopId,storeId,date)]

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
h[,.N,materialId_04][,.N]

orderSales[internal == "Нет",.(topSales = sum(order)),.(skuId)][order(-topSales)]

t <- h[internal == "Нет" & orderSkuId == "f92e1832-5448-11e8-8c7b-2c768a5115e1"]
f <- prod[skuId == "f92e1832-5448-11e8-8c7b-2c768a5115e1"]
y <- specification[skuId == "f92e1832-5448-11e8-8c7b-2c768a5115e1"]
t <- h[orderSkuId == "f92e1832-5448-11e8-8c7b-2c768a5115e1"]
rowProd[materialId == "156e8e4a-3e64-11e9-9800-2c768a5115e1"]
s <- rowProd[materialId %in% r$materialId]
temp[,.N,materialId][,.N]
